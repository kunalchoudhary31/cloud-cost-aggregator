"""
AWS Cost Explorer collector
"""
from datetime import date
from typing import List
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError, BotoCoreError

from collectors.base_collector import BaseCollector, CostRecord
from config import AWSConfig


class AWSCollector(BaseCollector):
    """
    Collector for AWS costs using Cost Explorer API
    """

    def __init__(self, config: AWSConfig):
        """
        Initialize AWS collector

        Args:
            config: AWS configuration
        """
        super().__init__('aws')
        self.config = config
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize AWS Cost Explorer client"""
        try:
            self.client = boto3.client(
                'ce',
                aws_access_key_id=self.config.access_key_id,
                aws_secret_access_key=self.config.secret_access_key,
                region_name=self.config.region
            )
            self.logger.info("AWS Cost Explorer client initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize AWS client: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test AWS Cost Explorer API connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get cost for a date range (AWS requires start < end)
            from datetime import timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=7)
            self.client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime('%Y-%m-%d'),
                    'End': end_date.strftime('%Y-%m-%d')
                },
                Granularity='DAILY',
                Metrics=['UnblendedCost']
            )
            self.logger.info("AWS connection test successful")
            return True
        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"AWS connection test failed: {e}")
            return False

    def collect_costs(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """
        Collect AWS costs for the specified date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of CostRecord objects
        """
        self.logger.info(f"Collecting AWS costs from {start_date} to {end_date}")

        try:
            # AWS Cost Explorer requires end date to be exclusive (next day)
            # So we add 1 day to end_date
            from datetime import timedelta
            api_end_date = end_date + timedelta(days=1)

            response = self.client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime('%Y-%m-%d'),
                    'End': api_end_date.strftime('%Y-%m-%d')
                },
                Granularity='DAILY',
                Metrics=['UsageQuantity', 'BlendedCost', 'UnblendedCost'],
                Filter={
                    'Not': {
                        'Dimensions': {
                            'Key': 'RECORD_TYPE',
                            'Values': ['Credit', 'Refund']
                        }
                    }
                },
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ]
            )

            # Parse response and create cost records
            records = self._parse_response(response)

            self._log_collection_summary(start_date, end_date, records)

            return records

        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"Failed to collect AWS costs: {e}")
            raise

    def _parse_response(self, response: dict) -> List[CostRecord]:
        """
        Parse AWS Cost Explorer API response

        Args:
            response: API response

        Returns:
            List of CostRecord objects
        """
        records = []

        for result in response.get('ResultsByTime', []):
            # Parse date
            usage_date = date.fromisoformat(result['TimePeriod']['Start'])

            # Parse groups (services)
            for group in result.get('Groups', []):
                service_name = group['Keys'][0]

                # Use BlendedCost which shows actual usage cost before credits
                # If BlendedCost is also 0, fall back to UnblendedCost
                blended_cost = float(group['Metrics']['BlendedCost']['Amount'])
                unblended_cost = float(group['Metrics']['UnblendedCost']['Amount'])

                # Use the higher of the two (to capture actual usage even with credits)
                cost_amount = max(blended_cost, unblended_cost)

                # Skip zero-cost services
                if cost_amount == 0:
                    continue

                # Create cost record
                record = CostRecord(
                    cloud_provider='aws',
                    service_name=service_name,
                    cost_usd=self._normalize_cost(cost_amount),
                    usage_date=usage_date
                )
                records.append(record)

        return records
