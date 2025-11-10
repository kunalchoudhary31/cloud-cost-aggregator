"""
Cloud cost aggregator - orchestrates cost collection from all providers
"""
from datetime import date
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from sqlalchemy.dialects.postgresql import insert

from collectors.base_collector import CostRecord
from collectors.aws_collector import AWSCollector
from collectors.gcp_collector import GCPCollector
from collectors.azure_collector import AzureCollector
from database.connection import DatabaseManager
from database.models import CloudCost
from config import Config

logger = logging.getLogger(__name__)


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

        # Initialize collectors
        self.collectors = {
            'aws': AWSCollector(config.aws),
            'gcp': GCPCollector(config.gcp),
            'azure': AzureCollector(config.azure)
        }

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
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit collection tasks
            future_to_provider = {
                executor.submit(
                    self._collect_provider_costs,
                    provider,
                    start_date,
                    end_date
                ): provider
                for provider in providers
                if provider in self.collectors
            }

            # Gather results as they complete
            for future in as_completed(future_to_provider):
                provider = future_to_provider[future]
                try:
                    records = future.result()
                    results[provider] = records
                    logger.info(f"{provider.upper()}: Collected {len(records)} cost records")
                except Exception as e:
                    logger.error(f"{provider.upper()}: Failed to collect costs: {e}")
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
        collector = self.collectors.get(provider)
        if not collector:
            raise ValueError(f"Unknown provider: {provider}")

        return collector.collect_costs(start_date, end_date)

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
        logger.info(f"Starting cost aggregation for {start_date} to {end_date}")

        # Collect costs from all providers
        results = self.collect_all_costs(start_date, end_date, providers)

        # Flatten all records
        all_records = []
        for provider, records in results.items():
            all_records.extend(records)

        # Save to database
        saved_count = self.save_costs(all_records)

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

        logger.info(f"Aggregation complete: {stats}")

        return stats

    def test_all_connections(self) -> Dict[str, bool]:
        """
        Test connections to all cloud providers

        Returns:
            Dictionary mapping provider to connection status
        """
        logger.info("Testing connections to all cloud providers")

        results = {}
        for provider, collector in self.collectors.items():
            try:
                results[provider] = collector.test_connection()
            except Exception as e:
                logger.error(f"{provider.upper()}: Connection test error: {e}")
                results[provider] = False

        return results
