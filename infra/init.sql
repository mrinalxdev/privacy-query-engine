-- DP Analytics Demo Database
-- Seed data for all 5 pre-approved query templates

-- Users table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  age_group VARCHAR(20) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (username, age_group) VALUES
  ('alice', '18-24'), ('bob', '25-34'), ('carol', '18-24'),
  ('dave', '35-44'), ('eve', '25-34'), ('frank', '45-54'),
  ('grace', '18-24'), ('heidi', '25-34'), ('ivan', '55+'),
  ('judy', '35-44'), ('karl', '25-34'), ('laura', '18-24'),
  ('mike', '45-54'), ('nancy', '35-44'), ('oscar', '25-34'),
  ('peggy', '18-24'), ('quinn', '55+'), ('rob', '25-34'),
  ('sarah', '35-44'), ('trent', '45-54'), ('uma', '18-24'),
  ('victor', '25-34'), ('wendy', '35-44'), ('xena', '55+'),
  ('yancy', '25-34'), ('zara', '18-24');

-- Employees table
CREATE TABLE IF NOT EXISTS employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  department VARCHAR(50) NOT NULL,
  salary NUMERIC(10,2) NOT NULL,
  hired_at DATE DEFAULT CURRENT_DATE
);

INSERT INTO employees (name, department, salary) VALUES
  ('Alice A', 'Engineering', 120000), ('Bob B', 'Engineering', 115000),
  ('Carol C', 'Engineering', 130000), ('Dave D', 'Marketing', 85000),
  ('Eve E', 'Marketing', 90000), ('Frank F', 'Sales', 75000),
  ('Grace G', 'Sales', 72000), ('Heidi H', 'Sales', 78000),
  ('Ivan I', 'HR', 65000), ('Judy J', 'HR', 68000),
  ('Karl K', 'Engineering', 125000), ('Laura L', 'Marketing', 88000),
  ('Mike M', 'Sales', 76000), ('Nancy N', 'Engineering', 118000),
  ('Oscar O', 'HR', 62000), ('Peggy P', 'Finance', 95000),
  ('Quinn Q', 'Finance', 100000), ('Rob R', 'Finance', 92000),
  ('Sarah S', 'Engineering', 135000), ('Trent T', 'Marketing', 82000);

-- Events table
CREATE TABLE IF NOT EXISTS events (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  event_type VARCHAR(50) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Generate events across last 30 days
DO $$
DECLARE
  i INTEGER;
  event_types TEXT[] := ARRAY['page_view', 'click', 'purchase', 'signup', 'logout'];
BEGIN
  FOR i IN 1..500 LOOP
    INSERT INTO events (user_id, event_type, created_at) VALUES (
      (FLOOR(RANDOM() * 26) + 1)::INTEGER,
      event_types[FLOOR(RANDOM() * 5 + 1)::INTEGER],
      NOW() - (FLOOR(RANDOM() * 30) || ' days')::INTERVAL
    );
  END LOOP;
END $$;


CREATE TABLE IF NOT EXISTS purchases (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  category VARCHAR(50) NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO purchases (user_id, category, amount)
SELECT
  (FLOOR(RANDOM() * 26) + 1)::INTEGER,
  (ARRAY['Electronics', 'Books', 'Clothing', 'Home', 'Sports', 'Food'])[FLOOR(RANDOM() * 6 + 1)::INTEGER],
  ROUND((RANDOM() * 499 + 1)::NUMERIC, 2)
FROM generate_series(1, 200);


CREATE TABLE IF NOT EXISTS cohorts (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  cohort_month DATE NOT NULL,
  returned INTEGER DEFAULT 0
);

INSERT INTO cohorts (user_id, cohort_month, returned)
SELECT
  (FLOOR(RANDOM() * 26) + 1)::INTEGER,
  DATE_TRUNC('month', NOW() - (FLOOR(RANDOM() * 6) || ' months')::INTERVAL)::DATE,
  (RANDOM() > 0.4)::INTEGER
FROM generate_series(1, 150);

SELECT 'Seed complete' AS status,
  (SELECT COUNT(*) FROM users) AS users,
  (SELECT COUNT(*) FROM employees) AS employees,
  (SELECT COUNT(*) FROM events) AS events,
  (SELECT COUNT(*) FROM purchases) AS purchases,
  (SELECT COUNT(*) FROM cohorts) AS cohorts;