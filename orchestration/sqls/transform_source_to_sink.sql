-- Create source table if it doesn't exist
DROP TABLE IF EXISTS source;
CREATE TABLE source (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
INSERT INTO source (name, created_at, updated_at) VALUES ('source', NOW(), NOW());

-- Create sink table if it doesn't exist
DROP TABLE IF EXISTS sink;
CREATE TABLE sink (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
-- Insert data from source to sink
INSERT INTO sink (name, created_at, updated_at)
SELECT 
    name,
    created_at,
    updated_at
FROM source
WHERE DATE(created_at) <= %(execution_date)s
ON CONFLICT (id) DO UPDATE
SET 
    name = EXCLUDED.name,
    updated_at = EXCLUDED.updated_at;
