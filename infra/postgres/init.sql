-- Postgres initial schema for Phase 3 (Orders/Inventory/Payments)
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    available INTEGER NOT NULL,
    reserved INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    line_no INTEGER NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    price_cents INTEGER NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE IF NOT EXISTS payments (
    order_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    transaction_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ
);

-- Idempotency keys per client request
CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    order_id UUID
);

-- Seed a couple of SKUs for demo
INSERT INTO inventory(sku, available, reserved)
VALUES
    ('SKU-RED-TSHIRT', 100, 0),
    ('SKU-BLUE-TSHIRT', 100, 0)
ON CONFLICT (sku) DO NOTHING;
