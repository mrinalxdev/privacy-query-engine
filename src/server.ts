/**
 *
 * Architecture:
 *   Bun/TS --> Laplace/Gaussian DP noise --> Redis budget tracking --> Kafka audit log --> PostgreSQL
 *
 * Usage todo after dinner:
 *   POST   /query         Run a DP-protected analytics query
 *   GET    /queries       List available query templates
 *   GET    /budget        Check remaining ε budget for a user
 *   GET    /budget/all    Admin: view all user budgets
 *   DELETE /budget        Admin: reset a user's budget
 *   GET    /health        Health check
 */

import {
  handleQuery,
  handleBudget,
  handleAllBudgets,
  handleResetBudget,
  handleListQueries,
  handleHealth,
} from "./routes";

const PORT = parseInt(process.env.PORT || "3000");

const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;
    const method = req.method;


    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-User-ID",
    };

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    try {
      let response: Response;

      if (method === "POST" && path === "/query") {
        response = await handleQuery(req);
      } else if (method === "GET" && path === "/queries") {
        response = await handleListQueries(req);
      } else if (method === "GET" && path === "/budget/all") {
        response = await handleAllBudgets(req);
      } else if (method === "GET" && path === "/budget") {
        response = await handleBudget(req);
      } else if (method === "DELETE" && path === "/budget") {
        response = await handleResetBudget(req);
      } else if (method === "GET" && path === "/health") {
        response = await handleHealth(req);
      } else {
        response = new Response(
          JSON.stringify({ ok: false, error: "Not found", available: ["/query", "/queries", "/budget", "/health"] }),
          { status: 404, headers: { "Content-Type": "application/json" } }
        );
      }


      Object.entries(corsHeaders).forEach(([k, v]) => response.headers.set(k, v));
      return response;
    } catch (err: any) {
      console.error("[Server] Unhandled error:", err);
      return new Response(
        JSON.stringify({ ok: false, error: "Internal server error" }),
        { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }
  },
  error(err) {
    console.error("[Server] Fatal:", err);
    return new Response("Internal Server Error", { status: 500 });
  },
});


console.log(`
                                 
 _____     _____                 
|  _  |___|     |_ _ ___ ___ _ _ 
|   __|___|  |  | | | -_|  _| | |
|__|      |__  _|___|___|_| |_  |
             |__|           |___|


ENDPOINTS 
POST   /query        │ DP query        
GET    /queries      │ List queries    
GET    /budget       │ Check ε budget 
GET    /budget/all   │ Admin: budgets 
DELETE /budget       │ Admin: reset   
GET    /health       │ Health check   

`);