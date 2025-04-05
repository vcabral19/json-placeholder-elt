CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    email TEXT,
    address JSONB,   -- Store the nested address as JSON
    phone TEXT,
    website TEXT,
    company JSONB,   -- Store the nested company info as JSON
    raw JSONB,       -- Optionally store the entire raw record for audit purposes
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
