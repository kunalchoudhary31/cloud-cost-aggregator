-- Cloud Cost Aggregator Database Schema

-- Drop table if exists (for clean setup)
DROP TABLE IF EXISTS cloud_costs;

-- Create cloud_costs table with upsert support
CREATE TABLE cloud_costs (
    id SERIAL PRIMARY KEY,
    cloud_provider VARCHAR(10) NOT NULL CHECK (cloud_provider IN ('aws', 'gcp', 'azure')),
    service_name VARCHAR(255) NOT NULL,
    cost_usd NUMERIC(15, 4) NOT NULL DEFAULT 0.0,
    usage_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint for upsert logic
    CONSTRAINT unique_cost_record UNIQUE (cloud_provider, service_name, usage_date)
);

-- Create indexes for better query performance
CREATE INDEX idx_cloud_costs_usage_date ON cloud_costs(usage_date);
CREATE INDEX idx_cloud_costs_provider ON cloud_costs(cloud_provider);
CREATE INDEX idx_cloud_costs_service ON cloud_costs(service_name);
CREATE INDEX idx_cloud_costs_provider_date ON cloud_costs(cloud_provider, usage_date);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to auto-update updated_at
CREATE TRIGGER update_cloud_costs_updated_at
    BEFORE UPDATE ON cloud_costs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for daily cost summaries by provider
CREATE OR REPLACE VIEW daily_cost_summary AS
SELECT
    usage_date,
    cloud_provider,
    SUM(cost_usd) as total_cost_usd,
    COUNT(DISTINCT service_name) as service_count
FROM cloud_costs
GROUP BY usage_date, cloud_provider
ORDER BY usage_date DESC, cloud_provider;

-- Create a view for service-level cost summaries
CREATE OR REPLACE VIEW service_cost_summary AS
SELECT
    cloud_provider,
    service_name,
    SUM(cost_usd) as total_cost_usd,
    COUNT(*) as day_count,
    AVG(cost_usd) as avg_daily_cost_usd,
    MIN(usage_date) as first_date,
    MAX(usage_date) as last_date
FROM cloud_costs
GROUP BY cloud_provider, service_name
ORDER BY total_cost_usd DESC;
