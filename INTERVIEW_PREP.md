# Interview Prep — SAP O2C Graph Explorer

Everything you need to explain this project confidently end-to-end.

---

## 1. One-Line Pitch

> "I built a full-stack system that ingests SAP Order-to-Cash data into a PostgreSQL graph, visualises the entity relationships in an interactive UI, and lets users query it using natural language — the LLM translates questions into SQL, executes them live, and answers with data-backed responses."

---

## 2. Understanding the Business Domain

### What is Order-to-Cash (O2C)?

O2C is the end-to-end business process from receiving a customer order to collecting payment. It's one of the most critical processes in any enterprise:

```
Customer places order
      ↓
Sales Order created (with line items)
      ↓
Goods picked, packed, shipped → Outbound Delivery
      ↓
Invoice sent to customer → Billing Document
      ↓
Accounting entries posted → Journal Entry (Accounts Receivable)
      ↓
Customer pays → Payment clears the open AR item
```

### Why does this matter in SAP?
- SAP stores each step in **separate tables** with FK relationships
- There is no "single view" of a transaction — you must JOIN across 5+ tables to trace one order
- AMS (Application Managed Services) teams spend enormous time manually tracing these chains to debug issues like "why wasn't this order billed?" or "where is my payment?"

### The Business Problem This Solves
Instead of a consultant manually JOINing tables in SAP transactions, a support engineer can type a natural language question and get the answer in seconds.

---

## 3. Data Model — Know This Cold

### The 19 Entity Types (grouped)

**Core O2C Flow:**
| Entity | Table | Key Column |
|---|---|---|
| Sales Order | `sales_order_headers` | `salesOrder` (PK) |
| Sales Order Item | `sales_order_items` | `(salesOrder, salesOrderItem)` |
| Outbound Delivery Header | `outbound_delivery_headers` | `deliveryDocument` (PK) |
| Outbound Delivery Item | `outbound_delivery_items` | `(deliveryDocument, deliveryDocumentItem)` |
| Billing Document Header | `billing_document_headers` | `billingDocument` (PK) |
| Billing Document Item | `billing_document_items` | `(billingDocument, billingDocumentItem)` |
| Journal Entry | `journal_entry_items_ar` | `(companyCode, fiscalYear, accountingDocument, item)` |
| Payment | `payments_ar` | `(companyCode, fiscalYear, accountingDocument, item)` |

**Master Data:**
| Entity | Table |
|---|---|
| Customer | `business_partners` |
| Customer Address | `business_partner_addresses` |
| Customer (Company) | `customer_company_assignments` |
| Customer (Sales Area) | `customer_sales_area_assignments` |
| Product | `products` |
| Product Description | `product_descriptions` |
| Product-Plant | `product_plants` |
| Product Storage | `product_storage_locations` |
| Plant | `plants` |
| Cancellations | `billing_document_cancellations` |

### The Critical JOIN Gotcha (interviewers will probe this)

**Common wrong assumption:** "Join delivery headers to sales orders directly"

**Reality:**
- `outbound_delivery_headers` has **no** `referenceSdDocument` column
- `billing_document_headers` has **no** `referenceSdDocument` column
- The linking columns live **only in the item tables**

```sql
-- CORRECT: SalesOrder → Delivery
JOIN outbound_delivery_items odi ON odi."referenceSdDocument" = soh."salesOrder"
JOIN outbound_delivery_headers odh ON odh."deliveryDocument" = odi."deliveryDocument"

-- CORRECT: Delivery → Billing
JOIN billing_document_items bdi ON bdi."referenceSdDocument" = odi."deliveryDocument"
JOIN billing_document_headers bdh ON bdh."billingDocument" = bdi."billingDocument"

-- CORRECT: Billing → Journal → Payment
JOIN journal_entry_items_ar je ON je."accountingDocument" = bdh."accountingDocument"
JOIN payments_ar pay ON pay."accountingDocument" = je."clearingAccountingDocument"
```

**Why?** This mirrors how SAP stores data — items carry the document reference, not headers. This is standard SAP design.

---

## 4. Architecture — Be Ready to Draw This

```
Browser (Vercel)
  │
  ├─ GraphView (Cytoscape.js)
  │    • Lazy loading: entity meta-nodes first
  │    • Click to expand records, click record to expand neighbours
  │    • Highlights nodes from chat response
  │
  ├─ ChatPanel
  │    • Sends {message, history[]} to backend
  │    • Shows response + SQL toggle + highlighted node badges
  │
  └─ NodeInspector
       • Shows all properties of selected node
       │
       ▼ REST API (CORS, no auth)
  FastAPI (Render/Docker)
  │
  ├─ GET /api/graph/schema → node types + counts + edge definitions
  ├─ GET /api/graph/nodes?type=X → paginated records
  ├─ GET /api/graph/expand?nodeType=X&nodeId=Y → node + all neighbours (SQL JOINs)
  └─ POST /api/chat → {message, history} → {response, sql, highlighted_nodes}
       │
       ▼ Two-stage LLM pipeline (Groq)
  Stage 1: Schema-aware SQL generation
  Stage 2: Result narration → JSON {response, highlighted_nodes}
       │
       ▼ psycopg2
  Neon PostgreSQL (cloud, free tier)
  19 tables, camelCase columns, FK indexes
```

### Why these specific choices?

**PostgreSQL over a graph DB (Neo4j, etc.):**
- The O2C relationships are deterministic FK joins, not variable-depth graph traversals
- SQL is the right tool when you know the schema upfront — no need for graph query language learning curve
- Single infra piece = simpler deployment

**Groq over OpenAI/Anthropic:**
- Free tier is sufficient for a demo
- ~1 second inference latency vs 3-5s on OpenAI
- Llama 3.3 70B is strong at SQL generation

**Cytoscape.js over D3/Sigma:**
- Purpose-built for graph visualisation
- Dagre layout plugin handles hierarchical O2C flow well
- Node expand/collapse, styling, and event handling built-in

**Neon over Supabase/local SQLite:**
- Serverless PG = accessible from any host (no VPN/IP whitelist)
- Render backend and Neon can talk over the public internet
- Free tier is generous enough for this dataset size

---

## 5. LLM Integration — The Most Important Part to Explain

### The Two-Stage Pipeline

**Stage 1 — SQL Generation**
```
Input: user question + schema context (system prompt) + last 3 conversation turns
Output: raw LLM text with a ```sql ... ``` block
```

**Stage 2 — Result Narration**
```
Input: original question + executed SQL + result rows (up to 30)
Output: JSON {response: "...", highlighted_nodes: [{type, id}, ...]}
```

Why two stages instead of one?
- Keeps SQL clean and testable
- Narration can reference actual data values (IDs, counts, names)
- Easier to debug — you can see exactly what SQL was generated

### Guardrail Design

**The guardrail is enforced in the system prompt:**
> "If the question is NOT about ERP/SAP/orders... respond EXACTLY with: 'This system is designed to answer questions related to the provided dataset only.'"

**Backend enforcement:**
```python
if GUARDRAIL_MARKER.lower() in llm_text.lower():
    return ChatResponse(response=GUARDRAIL_MARKER, sql=None)
    # SQL is never executed
```

**Why this approach?**
- Prompt-based guardrail is simpler than a separate classifier
- The `response EXACTLY with` instruction makes pattern matching reliable
- Stateless — no extra API calls or models needed

### Auto-Retry on SQL Failure

If the LLM generates SQL that fails:
1. Send the bad SQL + Postgres error message back to the LLM
2. Ask it to fix only the syntax (not the logic)
3. Re-execute — if it fails again, show a friendly message

```python
rows, error = execute_sql(conn, sql)
if error:
    fixed_sql = fix_sql(sql, error)  # one more Groq call
    if fixed_sql:
        rows, error = execute_sql(conn, fixed_sql)
```

### Column Quoting Safety Net

PostgreSQL lowercases unquoted identifiers. All columns in this schema are camelCase (e.g. `salesOrder`). So `SELECT salesOrder` fails — you need `SELECT "salesOrder"`.

Even with strong prompt instructions, LLMs sometimes forget quotes. Solution: post-process every generated SQL through `_quote_columns()` which wraps all known camelCase column names in double quotes programmatically before execution.

---

## 6. Frontend Architecture

### GraphView — How Lazy Loading Works

```
1. On mount:  GET /api/graph/schema
              → render 11 meta-nodes (entity types) with counts
              → apply dagre layout

2. Click meta-node (e.g. "SalesOrder"):
              → GET /api/graph/nodes?type=SalesOrder&limit=30
              → create record nodes, connect to meta-node
              → re-run layout

3. Click record node (e.g. salesOrder "740506"):
              → GET /api/graph/expand?nodeType=SalesOrder&nodeId=740506
              → backend runs JOIN queries: customer, items, products, plants
              → create neighbour nodes + edges
              → re-run layout

4. Chat response with highlighted_nodes:
              → cy.getElementById("SalesOrder:740506").addClass("highlighted")
              → node glows gold/amber
```

**Why lazy loading?** The full dataset has ~1,200 nodes. Rendering all at once makes Cytoscape unresponsive. Expanding on demand keeps the visible graph focused and fast.

### ChatPanel — State Management

```typescript
const [messages, setMessages] = useState<ChatMessage[]>([...]);
// Each message: { role, content, sql, highlighted_nodes }

// On send: append user message → POST /api/chat →
//   append assistant message → call onHighlight(highlighted_nodes)
//   parent App.tsx passes highlights to GraphView
```

The conversation `history` array (last N messages) is sent with every request so the LLM has context for follow-up questions.

---

## 7. Deployment

### Backend (Render + Docker)

The `Dockerfile` is minimal:
```dockerfile
FROM python:3.11-slim
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Render detects the Dockerfile, builds the image, and serves it with the env vars.

### Frontend (Vercel)

Vercel auto-detects Vite. Just set `VITE_API_URL=https://your-render-url.onrender.com`.
In local dev, `vite.config.ts` proxies `/api/*` to `localhost:8000` so no CORS issues during development.

---

## 8. Anticipated Interview Questions & Strong Answers

### "Why not use a proper graph database like Neo4j?"

> "The O2C relationships are deterministic and schema-fixed — every sales order links to items the same way, every delivery links to sales order items the same way. Neo4j shines for unknown-depth traversals or highly connected social graphs. Here, SQL JOINs are simpler, faster, and the LLM can generate SQL reliably. Adding Neo4j would double the infra surface area with no benefit."

### "How do you handle LLM hallucinations in SQL?"

> "Three layers: (1) the system prompt gives the full schema with explicit negative examples — 'this table does NOT have this column'; (2) the `_quote_columns()` post-processor fixes quoting issues before execution regardless of what the LLM produces; (3) if execution fails, we feed the Postgres error back to the LLM for one automatic fix attempt. Only if both fail do we return a user-friendly error."

### "What would you do to make this production-ready?"

> "Several things: (1) Read-only database user for the LLM-generated SQL — never give it write access; (2) SQL query allowlisting or a statement parser to block anything that isn't a SELECT; (3) rate limiting on the chat endpoint; (4) async psycopg3 instead of sync psycopg2 to avoid blocking workers; (5) proper conversation memory with session IDs instead of passing history in every request; (6) streaming responses from Groq for faster perceived latency."

### "How does the graph expansion work technically?"

> "When you click a SalesOrder node, the frontend calls `/api/graph/expand?nodeType=SalesOrder&nodeId=740506`. The backend runs several targeted JOIN queries — it fetches the business partner (customer), the sales order items, and for each item it fetches the product and plant. All results are returned as a flat list of `{type, id, properties}` nodes and `{source, target, label}` edges. Cytoscape adds them to the existing graph and re-runs the dagre layout."

### "Why not stream the LLM responses?"

> "I deprioritised it for the deadline but it's a natural next step. Groq supports streaming. The main change would be switching the `/api/chat` endpoint to a streaming FastAPI `StreamingResponse`, and the frontend would use `EventSource` or a `ReadableStream` to append tokens progressively. The SQL execution would still be blocking since we need to wait for the full SQL before running it."

### "The Customer node shows 0 — what's wrong?"

> "The business_partners table links to `soldToParty` on sales/billing headers. If the `businessPartner` IDs in `business_partners` don't match the `soldToParty` values in other tables, the join produces no results. This is a data quality issue — in a real SAP export, these would always match. The fix would be to cross-check the IDs and potentially add a fallback that shows customer IDs even without a `business_partners` match."

### "How would you scale this for a customer with millions of records?"

> "The current approach would break at scale in two ways: (1) the graph expansion queries might become slow — fix with materialized views or pre-computed adjacency tables; (2) Groq's free tier has rate limits — fix by caching common query patterns or using a paid tier. For the LLM, I'd also add a vector similarity search layer over the schema so the prompt only includes the most relevant tables for each query, rather than the full schema."

### "What did you learn from this project?"

> "The hardest problem wasn't the ML or the graph — it was the data modelling. The SAP schema is highly normalised: references between documents always live in item tables, not header tables. Getting that wrong causes silent empty results. I learned to validate JOIN paths by running them against real data before encoding them into prompts. I also learned that prompt engineering for SQL generation needs to be extremely precise — you can't rely on the LLM to remember quoting rules consistently, so programmatic post-processing is essential."

---

## 9. The 3 Example Queries — Understand These Deeply

### Query A: Products with most billing documents
```sql
SELECT bdi."material",
       COALESCE(pd."productDescription", bdi."material") AS product_name,
       COUNT(DISTINCT bdi."billingDocument") AS num_billing_docs
FROM billing_document_items bdi
LEFT JOIN product_descriptions pd
  ON pd."product" = bdi."material" AND pd."language" = 'EN'
GROUP BY bdi."material", pd."productDescription"
ORDER BY num_billing_docs DESC
LIMIT 20;
```
**Why it works:** Groups by material in billing items. LEFT JOIN means products without descriptions still appear (showing the raw ID). COALESCE picks the description if available.

### Query B: Delivered but not billed
```sql
SELECT DISTINCT soh."salesOrder", soh."soldToParty", soh."totalNetAmount"
FROM sales_order_headers soh
JOIN outbound_delivery_items odi ON odi."referenceSdDocument" = soh."salesOrder"
LEFT JOIN billing_document_items bdi ON bdi."referenceSdDocument" = odi."deliveryDocument"
WHERE bdi."billingDocument" IS NULL
LIMIT 50;
```
**Why it works:** The LEFT JOIN to billing means sales orders with deliveries but no matching billing item will have `bdi."billingDocument" IS NULL`. These are the "broken flow" records.

### Query C: Full O2C trace
```sql
SELECT soh."salesOrder", odi."deliveryDocument", bdh."billingDocument",
       je."accountingDocument", pay."accountingDocument" AS payment_doc
FROM billing_document_headers bdh
JOIN billing_document_items bdi ON bdi."billingDocument" = bdh."billingDocument"
JOIN outbound_delivery_items odi ON odi."deliveryDocument" = bdi."referenceSdDocument"
JOIN sales_order_headers soh ON soh."salesOrder" = odi."referenceSdDocument"
LEFT JOIN journal_entry_items_ar je ON je."accountingDocument" = bdh."accountingDocument"
LEFT JOIN payments_ar pay ON pay."accountingDocument" = je."clearingAccountingDocument"
WHERE bdh."billingDocument" = '90073895';
```
**Why it works:** Starts from the billing document and traverses both directions — backward to delivery and sales order, forward to journal entry and payment. LEFT JOINs on the finance tables mean the query returns partial chains even when payment hasn't been received yet.

---

## 10. What to Demo (Rehearse This)

**Demo flow (3-4 minutes):**

1. Open the app. Point out the entity meta-view — 11 node types with record counts. Explain this is the O2C data model.
2. Click "BillingHeader" → records expand. Click one billing header → its neighbours appear: billing items, journal entries, payment, customer.
3. Type in chat: *"Which products are associated with the highest number of billing documents?"* — show the SQL toggle, explain what the query does, notice highlighted nodes.
4. Type: *"Write me a poem"* — guardrail fires, show the canned response.
5. Type: *"Identify sales orders that were delivered but not billed"* — shows the broken flow analysis.

**What to emphasise:** "The answer is grounded in live data — not hallucinated. You can verify it by clicking 'Show SQL' and seeing the exact query that was run."

---

## 11. Key Numbers to Remember

| Metric | Value |
|---|---|
| Entity types | 19 tables |
| Graph node types | 11 |
| LLM model | Groq Llama 3.3 70B |
| Prompt size | ~4,000 tokens (schema + examples) |
| Response latency | ~2-3s end-to-end |
| SQL auto-retry | 1 attempt with error feedback |
| Max result rows fed to narrator | 30 |
| Max rows in query | 50 default |
| Context window used | ~5k of 128k available |

---

## 12. What You Would Add With More Time

Be ready to discuss improvements — it shows engineering maturity:

1. **Streaming responses** — tokens appear as they're generated, better UX
2. **Conversation memory** — session-scoped history stored server-side, not in every request
3. **SQL injection protection** — parse/validate that LLM only generates SELECT statements
4. **Semantic search** — embed product descriptions and customer names for fuzzy search
5. **Graph clustering** — group nodes by salesOrg or fiscal year to visualise at scale
6. **Schema change detection** — alert when new columns appear in JSONL exports
7. **Incremental ingestion** — process only new/changed records, not full reload
8. **Node highlighting persistence** — keep nodes highlighted across chat turns
