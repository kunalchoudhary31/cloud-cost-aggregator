"""
Base collector class for cloud cost collection
"""
from abc import ABC, abstractmethod
from datetime import date
from decimal import Decimal
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class CostRecord:
    """
    Standardized cost record format for all cloud providers
    """

    def __init__(
        self,
        cloud_provider: str,
        service_name: str,
        cost_usd: Decimal,
        usage_date: date
    ):
        self.cloud_provider = cloud_provider
        self.service_name = service_name
        self.cost_usd = cost_usd
        self.usage_date = usage_date

    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        return {
            'cloud_provider': self.cloud_provider,
            'service_name': self.service_name,
            'cost_usd': float(self.cost_usd),
            'usage_date': self.usage_date
        }

    def __repr__(self):
        return (
            f"<CostRecord({self.cloud_provider}, {self.service_name}, "
            f"${self.cost_usd}, {self.usage_date})>"
        )


class BaseCollector(ABC):
    """
    Abstract base class for cloud cost collectors
    """

    def __init__(self, provider_name: str):
        """
        Initialize collector

        Args:
            provider_name: Cloud provider name ('aws', 'gcp', 'azure')
        """
        self.provider_name = provider_name
        self.logger = logging.getLogger(f"{__name__}.{provider_name}")

    @abstractmethod
    def collect_costs(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """
        Collect costs for the specified date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of CostRecord objects

        Raises:
            Exception: If collection fails
        """
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test API connection and credentials

        Returns:
            True if connection successful, False otherwise
        """
        pass

    def _normalize_cost(self, cost: float) -> Decimal:
        """
        Normalize cost to Decimal with 4 decimal places

        Args:
            cost: Cost as float

        Returns:
            Cost as Decimal
        """
        return Decimal(str(round(cost, 4)))

    def _log_collection_summary(
        self,
        start_date: date,
        end_date: date,
        records: List[CostRecord]
    ):
        """
        Log summary of collected costs

        Args:
            start_date: Start date
            end_date: End date
            records: List of cost records
        """
        total_cost = sum(record.cost_usd for record in records)
        unique_services = len(set(record.service_name for record in records))

        self.logger.info(
            f"{self.provider_name.upper()}: Collected {len(records)} records "
            f"for {start_date} to {end_date}. "
            f"Total cost: ${total_cost:.2f} USD across {unique_services} services"
        )
