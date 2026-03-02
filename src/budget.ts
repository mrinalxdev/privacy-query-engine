import { createClient } from "redis";

import {readFileSync} from "fs"
import { fileURLToPath } from "url";
import path from "path";

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const LUA_SCRIPT = readFileSync(
  path.join(__dirname, "lua/budget.lua"), 
  "utf-8"
);

export interface BudgetEntry {
  epsilon_total: number;
  epsilon_spent: number;
  epsilon_remaining: number;
  query_count: number;
  last_query: string;
}

const DEFAULT_BUDGET = 10.0; 
const BUDGET_TTL_SECONDS = 86400; 

let client: ReturnType<typeof createClient> | null = null;

export async function getRedisClient() {
  if (!client) {
    client = createClient({
      url: process.env.REDIS_URL || "redis://localhost:6379",
    });
    client.on("error", (err) => console.error("[Redis] Error:", err));
    await client.connect();
    console.log("[Redis] Connected");
  }
  return client;
}

function budgetKey(userId: string) {
  return `dp:budget:${userId}`;
}


export async function getBudget(userId: string): Promise<BudgetEntry> {
  const redis = await getRedisClient();
  const raw = await redis.get(budgetKey(userId));

  if (!raw) {
    const entry: BudgetEntry = {
      epsilon_total: DEFAULT_BUDGET,
      epsilon_spent: 0,
      epsilon_remaining: DEFAULT_BUDGET,
      query_count: 0,
      last_query: new Date().toISOString(),
    };
    await redis.setEx(budgetKey(userId), BUDGET_TTL_SECONDS, JSON.stringify(entry));
    return entry;
  }

  return JSON.parse(raw) as BudgetEntry;
}

export async function deductBudget(
  userId: string,
  epsilonRequired: number
): Promise<BudgetEntry> {
  const redis = await getRedisClient();
  const key = budgetKey(userId);

  try {
    const result = await (redis as any).eval(LUA_SCRIPT, {
      keys: [key],
      arguments: [
        epsilonRequired.toString(),
        DEFAULT_BUDGET.toString(),
        new Date().toISOString(),
        BUDGET_TTL_SECONDS.toString(),
      ],
    });

    return JSON.parse(result as string) as BudgetEntry;
  } catch (err: any) {
    if (err.message?.includes("BUDGET_EXCEEDED")) {
      const current = await getBudget(userId);
      throw new BudgetExceededError(userId, epsilonRequired, current.epsilon_remaining);
    }
    throw err;
  }
}


export async function resetBudget(userId: string): Promise<void> {
  const redis = await getRedisClient();
  await redis.del(budgetKey(userId));
  console.log(`[Budget] Reset budget for user ${userId}`);
}


export async function getAllBudgets(): Promise<Record<string, BudgetEntry>> {
  const redis = await getRedisClient();
  const keys = await redis.keys("dp:budget:*");
  const result: Record<string, BudgetEntry> = {};

  for (const key of keys) {
    const raw = await redis.get(key);
    if (raw) {
      const userId = key.replace("dp:budget:", "");
      result[userId] = JSON.parse(raw) as BudgetEntry;
    }
  }

  return result;
}

export class BudgetExceededError extends Error {
  constructor(
    public userId: string,
    public required: number,
    public remaining: number
  ) {
    super(
      `Privacy budget exceeded for user '${userId}'. Required ε=${required}, remaining ε=${remaining.toFixed(4)}`
    );
    this.name = "BudgetExceededError";
  }
}