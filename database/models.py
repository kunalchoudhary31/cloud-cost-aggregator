"""
SQLAlchemy ORM models for cloud cost data
"""
from datetime import datetime
from decimal import Decimal
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, CheckConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class CloudCost(Base):
    """
    Model for storing cloud cost data from AWS, GCP, and Azure
    """
    __tablename__ = 'cloud_costs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cloud_provider = Column(String(10), nullable=False)
    service_name = Column(String(255), nullable=False)
    cost_usd = Column(Numeric(15, 4), nullable=False, default=0.0)
    usage_date = Column(Date, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, server_default=func.now())

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "cloud_provider IN ('aws', 'gcp', 'azure')",
            name='check_cloud_provider'
        ),
        UniqueConstraint(
            'cloud_provider', 'service_name', 'usage_date',
            name='unique_cost_record'
        ),
    )

    def __repr__(self):
        return (
            f"<CloudCost(id={self.id}, provider={self.cloud_provider}, "
            f"service={self.service_name}, cost=${self.cost_usd}, date={self.usage_date})>"
        )

    def to_dict(self):
        """Convert model instance to dictionary"""
        return {
            'id': self.id,
            'cloud_provider': self.cloud_provider,
            'service_name': self.service_name,
            'cost_usd': float(self.cost_usd) if self.cost_usd else 0.0,
            'usage_date': self.usage_date.isoformat() if self.usage_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
