INSERT INTO investors (
    investor_id, full_name, email, risk_profile, kyc_status,
    portfolio_value, preferences, qualified_investor, investment_horizon
)
SELECT
    gen_random_uuid(),
    'Investor ' || i,
    'investor' || i || '@example.com',
    CASE (i % 3)
        WHEN 0 THEN 'conservative'
        WHEN 1 THEN 'moderate'
        WHEN 2 THEN 'aggressive'
    END,
    CASE (i % 4)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'verified'
        WHEN 2 THEN 'verified'
        WHEN 3 THEN 'rejected'
    END,
    ROUND((random() * 1000000)::numeric, 2),
    jsonb_build_object(
        'currency', CASE (i % 3) WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' WHEN 2 THEN 'RUB' END,
        'notifications', (i % 2 = 0),
        'theme', CASE (i % 2) WHEN 0 THEN 'light' WHEN 1 THEN 'dark' END
    ),
    (i % 5 = 0),
    CASE (i % 3)
        WHEN 0 THEN 'short'
        WHEN 1 THEN 'medium'
        WHEN 2 THEN 'long'
    END
FROM generate_series(1, 10000) AS i
ON CONFLICT DO NOTHING;
