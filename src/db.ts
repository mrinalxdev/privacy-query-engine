import postgres from "postgres";

let sql: ReturnType<typeof postgres> | null = null;

export function getDb() {
  if (!sql) {
    sql = postgres(process.env.DATABASE_URL || "postgres://dp_user:dp_pass@localhost:5432/analytics", {
      max: 10,
      idle_timeout: 20,
      connect_timeout: 10,
    });
    console.log("[DB] PostgreSQL pool initialized");
  }
  return sql;
}

export interface QueryResult {
  rows: Record<string, unknown>[];
  numeric_columns: string[];
  query_type: string;
  sensitivity: number;
}


export const ALLOWED_QUERIES: Record<string, {
  sql: string;
  numeric_columns: string[];
  sensitivity: number;
  query_type: string;
  description: string;
}> = {
  "count_users_by_age": {
    sql: `SELECT age_group, COUNT(*) as user_count FROM users GROUP BY age_group ORDER BY age_group`,
    numeric_columns: ["user_count"],
    sensitivity: 1, 
    query_type: "count",
    description: "Count of users by age group",
  },
  "avg_salary_by_dept": {
    sql: `SELECT department, COUNT(*) as headcount, AVG(salary) as avg_salary, SUM(salary) as total_salary FROM employees GROUP BY department`,
    numeric_columns: ["headcount", "avg_salary", "total_salary"],
    sensitivity: 50000, 
    query_type: "aggregate",
    description: "Average salary statistics by department",
  },
  "daily_event_counts": {
    sql: `SELECT DATE_TRUNC('day', created_at) as day, event_type, COUNT(*) as event_count FROM events WHERE created_at > NOW() - INTERVAL '30 days' GROUP BY 1, 2 ORDER BY 1 DESC`,
    numeric_columns: ["event_count"],
    sensitivity: 1,
    query_type: "count",
    description: "Daily event counts over last 30 days",
  },
  "purchase_totals_by_category": {
    sql: `SELECT category, COUNT(*) as transaction_count, SUM(amount) as revenue, AVG(amount) as avg_order FROM purchases GROUP BY category`,
    numeric_columns: ["transaction_count", "revenue", "avg_order"],
    sensitivity: 1000, 
    query_type: "aggregate",
    description: "Purchase totals by product category",
  },
  "user_cohort_retention": {
    sql: `SELECT cohort_month, COUNT(DISTINCT user_id) as cohort_size, SUM(returned) as retained FROM cohorts GROUP BY cohort_month ORDER BY cohort_month`,
    numeric_columns: ["cohort_size", "retained"],
    sensitivity: 1,
    query_type: "count",
    description: "User cohort retention analysis",
  },
};

export async function runQuery(queryName: string): Promise<QueryResult> {
  const template = ALLOWED_QUERIES[queryName];
  if (!template) {
    throw new Error(`Unknown query: '${queryName}'. Allowed: ${Object.keys(ALLOWED_QUERIES).join(", ")}`);
  }

  const db = getDb();
  const start = Date.now();

  try {
    const rows = await db.unsafe(template.sql);
    const elapsed = Date.now() - start;
    console.log(`[DB] Query '${queryName}' returned ${rows.length} rows in ${elapsed}ms`);

    return {
      rows: rows as Record<string, unknown>[],
      numeric_columns: template.numeric_columns,
      sensitivity: template.sensitivity,
      query_type: template.query_type,
    };
  } catch (err: any) {
    throw new Error(`DB query failed: ${err.message}`);
  }
}

export async function closeDb() {
  if (sql) {
    await sql.end();
    sql = null;
  }
}