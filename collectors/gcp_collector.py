"""
GCP BigQuery Billing Export collector
"""
from datetime import date, timedelta
from typing import List
from decimal import Decimal
import os

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

from collectors.base_collector import BaseCollector, CostRecord
from config import GCPConfig


class GCPCollector(BaseCollector):
    """
    Collector for GCP costs using BigQuery billing export

    Requires:
    1. Billing export enabled in GCP Console
    2. BigQuery dataset with billing data
    """

    def __init__(self, config: GCPConfig):
        """
        Initialize GCP collector

        Args:
            config: GCP configuration
        """
        super().__init__('gcp')
        self.config = config
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize GCP BigQuery client"""
        try:

            # Create credentials and BigQuery client
            credentials = service_account.Credentials.from_service_account_file(
                self.config.credentials_path
            )

            self.client = bigquery.Client(
                project=self.config.project_id,
                credentials=credentials
            )
            self.logger.info("GCP BigQuery client initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP BigQuery client: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test GCP BigQuery connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # First check if dataset exists
            dataset_ref = self.client.get_dataset(
                f"{self.config.project_id}.{self.config.bigquery_dataset}"
            )
            self.logger.info(f"GCP BigQuery dataset '{self.config.bigquery_dataset}' exists")

            # Try to list tables in the dataset
            tables = list(self.client.list_tables(
                f"{self.config.project_id}.{self.config.bigquery_dataset}"
            ))

            if not tables:
                self.logger.warning(
                    "No billing export tables found yet. "
                    "It can take up to 24 hours for data to appear after enabling export. "
                    "Expected table pattern: gcp_billing_export_v1_*"
                )
                return True  # Dataset exists, just waiting for data

            # Try to query the billing export table
            query = f"""
                SELECT COUNT(*) as count
                FROM `{self.config.project_id}.{self.config.bigquery_dataset}.gcp_billing_export_v1_*`
                LIMIT 1
            """

            query_job = self.client.query(query)
            results = list(query_job.result())

            self.logger.info("GCP BigQuery connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"GCP connection test failed: {e}")
            self.logger.error(
                "Make sure BigQuery billing export is enabled. "
                "See: https://cloud.google.com/billing/docs/how-to/export-data-bigquery"
            )
            return False

    def collect_costs(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """
        Collect GCP costs for the specified date range from BigQuery

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of CostRecord objects
        """
        self.logger.info(f"Collecting GCP costs from {start_date} to {end_date}")

        try:
            # First, verify the dataset exists
            dataset_id = f"{self.config.project_id}.{self.config.bigquery_dataset}"
            try:
                dataset = self.client.get_dataset(dataset_id)
                self.logger.debug(f"Found dataset: {dataset_id} (location: {dataset.location})")
                
                # List tables in the dataset to help diagnose issues
                try:
                    tables = list(self.client.list_tables(dataset_id))
                    if tables:
                        table_names = [t.table_id for t in tables]
                        self.logger.debug(f"Found {len(tables)} table(s) in dataset: {', '.join(table_names)}")
                        
                        # Check if any tables match the billing export pattern
                        billing_tables = [t for t in table_names if t.startswith('gcp_billing_export_v1_')]
                        if not billing_tables:
                            self.logger.warning(
                                f"No billing export tables found matching pattern 'gcp_billing_export_v1_*'. "
                                f"Found tables: {', '.join(table_names)}"
                            )
                            self.logger.warning(
                                "If billing export was recently enabled, it may take up to 24 hours for tables to appear."
                            )
                    else:
                        self.logger.warning(
                            f"Dataset '{self.config.bigquery_dataset}' exists but contains no tables. "
                            "Billing export may not be configured yet."
                        )
                except Exception as list_error:
                    self.logger.debug(f"Could not list tables: {list_error}")
                    
            except GoogleAPIError as dataset_error:
                error_msg = str(dataset_error)
                if "not found" in error_msg.lower() or "notFound" in error_msg:
                    self.logger.error(
                        f"BigQuery dataset '{self.config.bigquery_dataset}' not found in project '{self.config.project_id}'"
                    )
                    # Try to list available datasets to help user
                    try:
                        datasets = list(self.client.list_datasets())
                        if datasets:
                            dataset_names = [d.dataset_id for d in datasets]
                            self.logger.info(
                                f"Available datasets in project '{self.config.project_id}': {', '.join(dataset_names)}"
                            )
                            self.logger.warning(
                                f"Please set GCP_BIGQUERY_DATASET environment variable to one of the available datasets, "
                                f"or create the dataset '{self.config.bigquery_dataset}' if billing export is not yet configured."
                            )
                        else:
                            self.logger.warning(
                                f"No datasets found in project '{self.config.project_id}'. "
                                f"Please enable BigQuery billing export first."
                            )
                    except Exception as list_error:
                        self.logger.debug(f"Could not list datasets: {list_error}")
                    
                    self.logger.warning(
                        "Make sure billing export is enabled and configured correctly. "
                        "See: https://cloud.google.com/billing/docs/how-to/export-data-bigquery"
                    )
                    return []
                else:
                    # Re-raise if it's a different error
                    raise

            # Query BigQuery billing export for daily service-level costs
            # This query properly handles CUD costs, savings programs, and credits
            # Costs take a few hours to show up in BigQuery export, might take longer than 24 hours

            # Format dates for the query (YYYY-MM-DD format with timezone)
            start_datetime = f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z"
            # Add one day to end_date for exclusive upper bound
            end_datetime = f"{(end_date + timedelta(days=1)).strftime('%Y-%m-%d')}T00:00:00Z"

            query = f"""
                WITH
                  spend_cud_fee_skus AS (
                  SELECT
                    *
                  FROM
                    UNNEST(['5515-81A8-03A2']) AS fee_sku_id ),
                  cost_data AS (
                  SELECT
                    *,
                  IF
                    (sku.id IN (
                      SELECT
                        *
                      FROM
                        spend_cud_fee_skus), cost, 0) AS `spend_cud_fee_cost`,
                    cost - IFNULL(cost_at_effective_price_default, cost) AS `spend_cud_savings`,
                    IFNULL(cost_at_effective_price_default, cost) - cost_at_list AS `negotiated_savings`,
                    IFNULL( (
                      SELECT
                        SUM(CAST(c.amount AS NUMERIC))
                      FROM
                        UNNEST(credits) c
                      WHERE
                        c.type IN ('FEE_UTILIZATION_OFFSET')), 0) AS `cud_credits`,
                    IFNULL( (
                      SELECT
                        SUM(CAST(c.amount AS NUMERIC))
                      FROM
                        UNNEST(credits) c
                      WHERE
                        c.type IN ('SUSTAINED_USAGE_DISCOUNT', 'DISCOUNT')), 0) AS `other_savings`
                  FROM
                    `{self.config.project_id}.{self.config.bigquery_dataset}.gcp_billing_export_v1_*`
                  WHERE
                    cost_type != 'tax'
                    AND cost_type != 'adjustment'
                    AND usage_start_time >= TIMESTAMP('{start_datetime}')
                    AND usage_start_time < TIMESTAMP('{end_datetime}'))
                SELECT
                  DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS usage_date,
                  service.description AS service_name,
                  SUM(CAST(cost AS NUMERIC)) + SUM(CAST(cud_credits AS NUMERIC)) + SUM(CAST(other_savings AS NUMERIC))
                    AS cost_usd
                FROM
                  cost_data
                GROUP BY
                  usage_date,
                  service_name
                HAVING
                  ABS(cost_usd) > 0.01
                ORDER BY
                  usage_date DESC,
                  cost_usd DESC
            """

            self.logger.debug(f"Executing BigQuery query: {query}")

            # Execute query
            query_job = self.client.query(query)
            results = query_job.result()

            # Parse results
            records = []
            for row in results:
                record = CostRecord(
                    cloud_provider='gcp',
                    service_name=row.service_name or 'Unknown',
                    cost_usd=self._normalize_cost(float(row.cost_usd)),
                    usage_date=row.usage_date
                )
                records.append(record)

            self._log_collection_summary(start_date, end_date, records)

            return records

        except GoogleAPIError as e:
            error_msg = str(e)
            if "does not have a schema" in error_msg.lower():
                self.logger.warning(
                    "GCP billing export table exists but has no schema (empty table). "
                    "This usually means billing export is configured but hasn't received data yet."
                )
                # Extract table name from error if possible
                if "gcp_billing_export_v1_" in error_msg:
                    # Try to extract the table name
                    import re
                    table_match = re.search(r'gcp_billing_export_v1_\w+', error_msg)
                    if table_match:
                        table_name = table_match.group(0)
                        self.logger.info(f"Found empty table: {table_name}")
                self.logger.warning(
                    "Billing export tables are created when you enable export, but they remain empty until "
                    "GCP starts exporting billing data. This can take up to 24-48 hours after enabling export."
                )
                self.logger.info(
                    "To verify billing export is working:\n"
                    "  1. Go to GCP Console > Billing > Export\n"
                    "  2. Check that export is enabled and pointing to the correct BigQuery dataset\n"
                    "  3. Wait for billing data to appear (usually within 24-48 hours)"
                )
                return []
            elif "does not match any table" in error_msg or "not match any table" in error_msg:
                self.logger.warning(
                    "GCP billing export tables not found matching pattern 'gcp_billing_export_v1_*'. "
                    "It can take up to 24 hours for data to appear after enabling export."
                )
                # Try to list actual tables to help diagnose
                try:
                    dataset_id = f"{self.config.project_id}.{self.config.bigquery_dataset}"
                    tables = list(self.client.list_tables(dataset_id))
                    if tables:
                        table_names = [t.table_id for t in tables]
                        self.logger.info(f"Found tables in dataset: {', '.join(table_names)}")
                        self.logger.warning(
                            "If you see tables with different names, the billing export may use a different format. "
                            "Check your GCP Console billing export configuration."
                        )
                except Exception as list_error:
                    self.logger.debug(f"Could not list tables for diagnostics: {list_error}")
            elif "not found" in error_msg.lower() or "notFound" in error_msg:
                # Dataset not found error - already handled above, but catch here too
                self.logger.error(f"Failed to collect GCP costs via BigQuery: {e}")
                self.logger.warning(
                    "Make sure billing export is enabled and configured correctly. "
                    "See: https://cloud.google.com/billing/docs/how-to/export-data-bigquery"
                )
            else:
                self.logger.error(f"Failed to collect GCP costs via BigQuery: {e}")
                self.logger.warning(
                    "Make sure billing export is enabled and configured correctly. "
                    "See: https://cloud.google.com/billing/docs/how-to/export-data-bigquery"
                )
            # Return empty list instead of failing completely
            return []
        except Exception as e:
            self.logger.error(f"Failed to collect GCP costs: {e}")
            raise
