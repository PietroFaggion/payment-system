--------------- seed data START ----------------
-- Seed accounts
INSERT INTO accounts (id, owner_name, balance, currency, created_at, version)
VALUES
    (1001, 'Alice Doe', 1500.0000, 'EUR', now(), 0),
    (1002, 'Bob Roe', 750.0000, 'USD', now(), 0),
    (1003, 'Charlie Poe', 300.0000, 'EUR', now(), 0);
