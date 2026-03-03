import { runQuery, ALLOWED_QUERIES } from "./db";
import { applyDPToRows, laplaceMechanism, gaussianMechanism } from "./privacy";
import { deductBudget, getBudget, resetBudget, getAllBudgets, BudgetExceededError } from "./budget";
import { logAuditEvent, hashQuery, generateEventId, type AuditEvent } from "./kafka";

export interface ApiResponse {
  ok: boolean;
  data?: unknown;
  error?: string;
  meta?: Record<string, unknown>;
}

function json(data: ApiResponse, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function parseUserId(req: Request): string | null {
  return req.headers.get("X-User-ID") || new URL(req.url).searchParams.get("user_id");
}


export async function handleQuery(req: Request): Promise<Response> {
  const userId = parseUserId(req);
  if (!userId) return json({ ok: false, error: "Missing X-User-ID header or user_id param" }, 401);

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ ok: false, error: "Invalid JSON body" }, 400);
  }

  const { query: queryName, epsilon, mechanism = "laplace", delta } = body;

  if (!queryName) return json({ ok: false, error: "Missing 'query' field" }, 400);
  if (!epsilon || typeof epsilon !== "number" || epsilon <= 0 || epsilon > 5) {
    return json({ ok: false, error: "epsilon must be a positive number ≤ 5" }, 400);
  }
  if (mechanism === "gaussian" && (!delta || delta <= 0 || delta >= 0.01)) {
    return json({ ok: false, error: "Gaussian mechanism requires delta ∈ (0, 0.01)" }, 400);
  }

  const timestamp = new Date().toISOString();
  const eventId = generateEventId(userId, timestamp);
  const queryHash = await hashQuery(queryName);

  let budgetBefore = 0;
  let budgetEntry;

  try {
 
    const currentBudget = await getBudget(userId);
    budgetBefore = currentBudget.epsilon_remaining;
    budgetEntry = await deductBudget(userId, epsilon);
    const queryResult = await runQuery(queryName);
    const { rows: noisyRows, epsilon_used } = applyDPToRows(
      queryResult.rows,
      queryResult.numeric_columns,
      { epsilon, sensitivity: queryResult.sensitivity, delta },
      mechanism
    );

    const auditEvent: AuditEvent = {
      event_id: eventId,
      timestamp,
      user_id: userId,
      query_hash: queryHash,
      query_type: queryResult.query_type,
      mechanism,
      epsilon_used,
      delta_used: delta || null,
      sensitivity: queryResult.sensitivity,
      noise_scale: mechanism === "laplace"
        ? queryResult.sensitivity / epsilon
        : (queryResult.sensitivity * Math.sqrt(2 * Math.log(1.25 / (delta || 1e-5)))) / epsilon,
      budget_before: budgetBefore,
      budget_after: budgetEntry.epsilon_remaining,
      result_row_count: noisyRows.length,
      numeric_columns: queryResult.numeric_columns,
      status: "success",
    };
    logAuditEvent(auditEvent); 

    return json({
      ok: true,
      data: noisyRows,
      meta: {
        event_id: eventId,
        mechanism,
        epsilon_used,
        delta_used: delta || null,
        budget_remaining: budgetEntry.epsilon_remaining,
        budget_total: budgetEntry.epsilon_total,
        query_count: budgetEntry.query_count,
        row_count: noisyRows.length,
        numeric_columns: queryResult.numeric_columns,
        dp_guarantee: mechanism === "laplace"
          ? `ε-DP with ε=${epsilon}`
          : `(ε,δ)-DP with ε=${epsilon}, δ=${delta}`,
      },
    });
  } catch (err: any) {
    
    const auditEvent: AuditEvent = {
      event_id: eventId,
      timestamp,
      user_id: userId,
      query_hash: queryHash,
      query_type: "unknown",
      mechanism,
      epsilon_used: err instanceof BudgetExceededError ? 0 : epsilon,
      delta_used: delta || null,
      sensitivity: 0,
      noise_scale: 0,
      budget_before: budgetBefore,
      budget_after: budgetBefore,
      result_row_count: 0,
      numeric_columns: [],
      status: err instanceof BudgetExceededError ? "budget_exceeded" : "error",
      error: err.message,
    };
    logAuditEvent(auditEvent);

    if (err instanceof BudgetExceededError) {
      return json({ ok: false, error: err.message, meta: { budget_remaining: err.remaining } }, 429);
    }
    return json({ ok: false, error: err.message }, 500);
  }
}


export async function handleBudget(req: Request): Promise<Response> {
  const userId = parseUserId(req);
  if (!userId) return json({ ok: false, error: "Missing user_id" }, 401);

  const budget = await getBudget(userId);
  return json({ ok: true, data: budget });
}

export async function handleAllBudgets(_req: Request): Promise<Response> {
  const budgets = await getAllBudgets();
  return json({ ok: true, data: budgets });
}

export async function handleResetBudget(req: Request): Promise<Response> {
  const userId = parseUserId(req);
  if (!userId) return json({ ok: false, error: "Missing user_id" }, 401);
  await resetBudget(userId);
  return json({ ok: true, data: { message: `Budget reset for ${userId}` } });
}


export async function handleListQueries(_req: Request): Promise<Response> {
  const queries = Object.entries(ALLOWED_QUERIES).map(([name, q]) => ({
    name,
    description: q.description,
    query_type: q.query_type,
    numeric_columns: q.numeric_columns,
    sensitivity: q.sensitivity,
  }));
  return json({ ok: true, data: queries });
}

export async function handleHealth(_req: Request): Promise<Response> {
  return json({
    ok: true,
    data: {
      status: "healthy",
      service: "dp-analytics-api",
      version: "1.0.0",
      timestamp: new Date().toISOString(),
    },
  });
}