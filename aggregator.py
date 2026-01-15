"""
Cloud cost aggregator - orchestrates cost collection from all providers
"""
from datetime import date
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys

from sqlalchemy.dialects.postgresql import insert

from collectors.base_collector import CostRecord
from collectors.aws_collector import AWSCollector
from collectors.gcp_collector import GCPCollector
from collectors.azure_collector import AzureCollector
from database.connection import DatabaseManager
from database.models import CloudCost
from config import Config
from utils.logger import get_logger

logger = get_logger('cloud_cost_aggregator')


class CostAggregator:
    """
    Orchestrates cost collection from multiple cloud providers
    and stores results in PostgreSQL
    """

    def __init__(self, config: Config, db_manager: DatabaseManager):
        """
        Initialize cost aggregator

        Args:
            config: Application configuration
            db_manager: Database manager instance
        """
        self.config = config
        self.db_manager = db_manager

        # Lazy initialization - collectors will be created when needed
        self._collectors = {}
        self._collector_classes = {
            'aws': AWSCollector,
            'gcp': GCPCollector,
            'azure': AzureCollector
        }
        self._collector_configs = {
            'aws': config.aws,
            'gcp': config.gcp,
            'azure': config.azure
        }
    
    @property
    def collectors(self):
        """Lazy-loaded collectors dictionary"""
        # Initialize all collectors if not already done
        if not self._collectors:
            for provider, collector_class in self._collector_classes.items():
                try:
                    self._collectors[provider] = collector_class(self._collector_configs[provider])
                except Exception as e:
                    logger.warning(f"Failed to initialize {provider} collector: {e}")
                    # Store None to indicate initialization failure
                    self._collectors[provider] = None
        return self._collectors
    
    def _get_collector(self, provider: str):
        """Get a collector for a specific provider, initializing it if needed"""
        if provider not in self._collectors:
            if provider not in self._collector_classes:
                raise ValueError(f"Unknown provider: {provider}")
            try:
                self._collectors[provider] = self._collector_classes[provider](
                    self._collector_configs[provider]
                )
            except Exception as e:
                logger.warning(f"Failed to initialize {provider} collector: {e}")
                self._collectors[provider] = None
        
        collector = self._collectors.get(provider)
        if collector is None:
            raise ValueError(f"Collector for {provider} failed to initialize")
        return collector

    def collect_all_costs(
        self,
        start_date: date,
        end_date: date,
        providers: List[str] = None
    ) -> Dict[str, List[CostRecord]]:
        """
        Collect costs from all (or specified) cloud providers in parallel

        Args:
            start_date: Start date
            end_date: End date
            providers: Optional list of provider names to collect from
                      If None, collect from all providers

        Returns:
            Dictionary mapping provider name to list of cost records
        """
        if providers is None:
            providers = ['aws', 'gcp', 'azure']

        logger.info(f"Collecting costs from providers: {providers}")

        results = {}
        errors = {}

        # Collect costs in parallel
        logger.info(f"Submitting collection tasks for {len(providers)} provider(s)...")
        logger.info("Creating ThreadPoolExecutor...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            logger.info("ThreadPoolExecutor created")
            # Submit collection tasks
            future_to_provider = {}
            for provider in providers:
                if provider in self._collector_classes:
                    logger.info(f"Submitting collection task for {provider.upper()}...")
                    future = executor.submit(
                        self._collect_provider_costs,
                        provider,
                        start_date,
                        end_date
                    )
                    future_to_provider[future] = provider
                    logger.info(f"Collection task submitted for {provider.upper()}")
                else:
                    logger.warning(f"Skipping unknown provider: {provider}")

            logger.info(f"Waiting for {len(future_to_provider)} collection task(s) to complete...")
            
            # Gather results as they complete
            completed = 0
            for future in as_completed(future_to_provider):
                provider = future_to_provider[future]
                completed += 1
                logger.info(f"[{completed}/{len(future_to_provider)}] Processing results for {provider.upper()}...")
                try:
                    logger.info(f"Waiting for {provider.upper()} collection to finish...")
                    records = future.result()
                    results[provider] = records
                    logger.info(f"{provider.upper()}: ✓ Successfully collected {len(records)} cost records")
                except Exception as e:
                    logger.error(f"{provider.upper()}: ✗ Failed to collect costs: {e}", exc_info=True)
                    errors[provider] = str(e)
                    results[provider] = []

        # Log summary
        total_records = sum(len(records) for records in results.values())
        logger.info(
            f"Collection complete: {total_records} total records from "
            f"{len(results)} providers. Errors: {len(errors)}"
        )

        if errors:
            logger.warning(f"Errors during collection: {errors}")

        return results

    def _collect_provider_costs(
        self,
        provider: str,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """
        Collect costs from a single provider

        Args:
            provider: Provider name
            start_date: Start date
            end_date: End date

        Returns:
            List of cost records
        """
        logger.info(f"[{provider.upper()}] Starting cost collection for date range: {start_date} to {end_date}")
        try:
            logger.info(f"[{provider.upper()}] Getting collector instance...")
            collector = self._get_collector(provider)
            logger.info(f"[{provider.upper()}] Collector instance obtained, calling collect_costs()...")
            records = collector.collect_costs(start_date, end_date)
            logger.info(f"[{provider.upper()}] Collection completed, returning {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"[{provider.upper()}] Error in _collect_provider_costs: {e}", exc_info=True)
            raise

    def save_costs(self, cost_records: List[CostRecord]) -> int:
        """
        Save cost records to database with upsert logic

        Args:
            cost_records: List of cost records to save

        Returns:
            Number of records saved/updated
        """
        if not cost_records:
            logger.info("No cost records to save")
            return 0

        logger.info(f"Saving {len(cost_records)} cost records to database")

        try:
            with self.db_manager.get_session() as session:
                # Convert CostRecord objects to dictionaries
                records_data = [
                    {
                        'cloud_provider': record.cloud_provider,
                        'service_name': record.service_name,
                        'cost_usd': record.cost_usd,
                        'usage_date': record.usage_date
                    }
                    for record in cost_records
                ]

                # Perform upsert (insert with on conflict update)
                stmt = insert(CloudCost).values(records_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['cloud_provider', 'service_name', 'usage_date'],
                    set_={
                        'cost_usd': stmt.excluded.cost_usd,
                        'updated_at': stmt.excluded.updated_at
                    }
                )

                result = session.execute(stmt)
                session.commit()

                logger.info(f"Successfully saved/updated {len(cost_records)} cost records")
                return len(cost_records)

        except Exception as e:
            logger.error(f"Failed to save cost records: {e}")
            raise

    def aggregate_and_store(
        self,
        start_date: date,
        end_date: date,
        providers: List[str] = None
    ) -> Dict[str, int]:
        """
        Main aggregation method: collect costs and store in database

        Args:
            start_date: Start date
            end_date: End date
            providers: Optional list of providers to collect from

        Returns:
            Dictionary with statistics
        """
        logger.info(">>> ENTERED aggregate_and_store() method")
        
        logger.info("=" * 60)
        logger.info(f"Starting cost aggregation for {start_date} to {end_date}")
        if providers:
            logger.info(f"Providers to process: {providers}")
        logger.info("=" * 60)

        # Collect costs from all providers
        logger.info("Step 1: Collecting costs from providers...")
        logger.info("About to call collect_all_costs()...")
        results = self.collect_all_costs(start_date, end_date, providers)
        logger.info(f"Step 1 complete: Collected data from {len(results)} provider(s)")

        # Flatten all records
        logger.info("Step 2: Flattening collected records...")
        all_records = []
        for provider, records in results.items():
            all_records.extend(records)
            logger.info(f"  - {provider.upper()}: {len(records)} records")
        logger.info(f"Total records to save: {len(all_records)}")

        # Save to database
        logger.info("Step 3: Saving records to database...")
        saved_count = self.save_costs(all_records)
        logger.info(f"Step 3 complete: Saved {saved_count} record(s) to database")

        # Calculate statistics
        stats = {
            'total_records': len(all_records),
            'saved_records': saved_count,
            'providers_succeeded': len([p for p, r in results.items() if r]),
            'providers_failed': len([p for p, r in results.items() if not r])
        }

        # Calculate total cost by provider
        for provider, records in results.items():
            total_cost = sum(record.cost_usd for record in records)
            stats[f'{provider}_cost_usd'] = float(total_cost)
            stats[f'{provider}_records'] = len(records)

        logger.info("=" * 60)
        logger.info("Aggregation statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        logger.info("=" * 60)
        logger.info("Cost aggregation completed successfully")

        return stats

    def test_all_connections(self, providers: List[str] = None) -> Dict[str, bool]:
        """
        Test connections to cloud providers

        Args:
            providers: Optional list of provider names to test.
                      If None, test all providers

        Returns:
            Dictionary mapping provider to connection status
        """
        if providers is None:
            providers = ['aws', 'gcp', 'azure']
        
        logger.info(f"Testing connections to providers: {providers}")

        results = {}
        for provider in providers:
            if provider not in self._collector_classes:
                logger.warning(f"Unknown provider: {provider}, skipping")
                results[provider] = False
                continue
            
            try:
                logger.info(f"Initializing {provider.upper()} collector...")
                collector = self._get_collector(provider)
                logger.info(f"Testing {provider.upper()} connection...")
                results[provider] = collector.test_connection()
                logger.info(f"{provider.upper()} connection test completed: {results[provider]}")
            except Exception as e:
                logger.error(f"{provider.upper()}: Connection test error: {e}", exc_info=True)
                results[provider] = False

        return results
