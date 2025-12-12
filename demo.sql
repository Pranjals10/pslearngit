-- demo.sql
CREATE TABLE IF NOT EXIST users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  email TEX
);

INSERT INTO users (id, name, email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com');
