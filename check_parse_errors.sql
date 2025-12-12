-- check_parse_errors.sql
-- Intentional SQL parsing issues and PII exposure for testing.
-- Connection string accidentally pasted here:
-- DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=EXPOSED_KEY;EndpointSuffix=core.windows.net

CREAT TABLE customers (   -- misspelled CREATE
  id INT PRIMARY KEY,
  full_name TEXT,
  email TEXT
);

-- Malformed INSERTs and mismatched columns
INSERT INTO customers (id, full_name, email) VALUES
  (1, 'Alice Smith', 'alice.smith@example.com'),
  (2, 'Bob Jones');  -- missing email value

-- Potential SQL injection pattern (concatenated string)
-- Note: This is intentionally bad for testing only.
SELECT * FROM customers WHERE email = '" || user_input || "';
