CREATE TABLE IF NOT EXISTS investors (
    investor_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name          VARCHAR(255) NOT NULL,
    email              VARCHAR(255) UNIQUE NOT NULL,
    risk_profile       VARCHAR(50) NOT NULL DEFAULT 'moderate',
    kyc_status         VARCHAR(50) NOT NULL DEFAULT 'pending',
    portfolio_value    DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    preferences        JSONB DEFAULT '{}',
    qualified_investor BOOLEAN NOT NULL DEFAULT FALSE,
    investment_horizon VARCHAR(16) NOT NULL DEFAULT 'medium',
    cache_version      BIGINT NOT NULL DEFAULT 1,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT now(),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL DEFAULT 'investor',
    event_type      VARCHAR(50) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT now(),
    published       BOOLEAN NOT NULL DEFAULT false,
    published_at    TIMESTAMP WITH TIME ZONE,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(published) WHERE published = false;

CREATE OR REPLACE FUNCTION increment_cache_version()
RETURNS TRIGGER AS $$
BEGIN
    NEW.cache_version := OLD.cache_version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_increment_cache_version ON investors;
CREATE TRIGGER trg_increment_cache_version
    BEFORE UPDATE ON investors
    FOR EACH ROW
    EXECUTE FUNCTION increment_cache_version();
