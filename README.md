# SAP O2C Graph Explorer

A **context graph system with an LLM-powered query interface** over SAP Order-to-Cash data.

**Live Demo**: [TODO — add Vercel URL]  
**GitHub**: [this repo]

---

## What It Does

Enterprise ERP data is spread across dozens of disconnected tables — orders, deliveries, invoices, payments — with no easy way to trace how they connect. This system:

- **Ingests** 19 SAP O2C entity types from raw JSONL exports into a structured PostgreSQL database
- **Models** the full Order-to-Cash flow as a graph (nodes = business entities, edges = relationships)
- **Visualises** that graph interactively — click any entity type to explore records, click a record to expand its neighbours across the O2C chain
- **Answers natural language questions** via an LLM chat interface that translates queries into SQL, executes them live, and returns data-backed answers with the relevant graph nodes highlighted
- **Guardrails** the LLM to only respond to dataset-relevant questions

---

## Architecture

```
┌──────────────────────────────────────────────┐
│         React + Vite + TypeScript             │  ← Vercel
│   ┌───────────────┐   ┌───────────────────┐  │
│   │  GraphView    │   │    ChatPanel      │  │
│   │ (Cytoscape.js)│   │  (LLM interface)  │  │
│   └───────────────┘   └───────────────────┘  │
└─────────────────────┬────────────────────────┘
                      │  REST  /api/*
┌─────────────────────▼────────────────────────┐
│           FastAPI  (Python 3.11)              │  ← Render (Docker)
│  GET  /api/graph/schema                       │
│  GET  /api/graph/nodes?type=&page=            │
│  GET  /api/graph/expand?nodeType=&nodeId=     │
│  POST /api/chat   ── Groq Llama 3.3 70B      │
└─────────────────────┬────────────────────────┘
                      │  psycopg2
┌─────────────────────▼────────────────────────┐
│         Neon  (PostgreSQL, cloud)             │  ← Neon free tier
│   19 tables · FK constraints · indexes       │
└──────────────────────────────────────────────┘
```

---

## Tech Stack & Decisions

| Layer | Choice | Why |
|---|---|---|
| Database | **Neon (PostgreSQL)** | Serverless cloud PG; accessible from Render without VPC setup; the O2C graph is derived from SQL JOINs — no separate graph DB needed |
| Graph model | **SQL JOINs** (not Neo4j/NetworkX) | FK relationships are well-defined; SQL is sufficient, more queryable, and adds zero infra |
| LLM | **Groq — Llama 3.3 70B** | Free tier, very low latency (~1s), 128k context fits the full schema prompt |
| Frontend graph | **Cytoscape.js + dagre** | Battle-tested, handles dynamic expansion, dagre layout works well for hierarchical O2C flows |
| Backend | **FastAPI** | Fast to develop, automatic OpenAPI docs, async-friendly for serving LLM responses |
| Deploy | **Vercel + Render + Neon** | All free tiers, zero config for HTTPS, matches spec (no auth required) |

---

## Graph Model

### O2C Flow (edge path)

```
Customer
   └── PLACED_BY ──► SalesOrder
                         └── HAS_ITEM ──► SalesOrderItem
                                              └── REFERENCES ──► Product
                                              └── PRODUCED_AT ──► Plant
                                              └── FULFILLED_BY ──► DeliveryItem
                                                                       └── BELONGS_TO ──► DeliveryHeader
                                                                       └── LEADS_TO ──► BillingItem
                                                                                            └── BELONGS_TO ──► BillingHeader
                                                                                                                   └── CREATES ──► JournalEntry
                                                                                                                                      └── CLEARED_BY ──► Payment
```

### Node Types (11)

| Node | Table | Record Count |
|---|---|---|
| SalesOrder | sales_order_headers | ~100 |
| SalesOrderItem | sales_order_items | ~167 |
| DeliveryHeader | outbound_delivery_headers | ~86 |
| DeliveryItem | outbound_delivery_items | ~137 |
| BillingHeader | billing_document_headers | ~163 |
| BillingItem | billing_document_items | ~245 |
| JournalEntry | journal_entry_items_ar | ~123 |
| Payment | payments_ar | ~120 |
| Customer | business_partners | ~8 |
| Product | products | ~69 |
| Plant | plants | ~44 |

### Critical JOIN Paths

```sql
-- SalesOrder → Delivery (must go through outbound_delivery_items)
JOIN outbound_delivery_items odi ON odi."referenceSdDocument" = soh."salesOrder"
JOIN outbound_delivery_headers odh ON odh."deliveryDocument" = odi."deliveryDocument"

-- Delivery → Billing (must go through billing_document_items)
JOIN billing_document_items bdi ON bdi."referenceSdDocument" = odi."deliveryDocument"
JOIN billing_document_headers bdh ON bdh."billingDocument" = bdi."billingDocument"

-- Billing → Journal → Payment
JOIN journal_entry_items_ar je ON je."accountingDocument" = bdh."accountingDocument"
JOIN payments_ar pay ON pay."accountingDocument" = je."clearingAccountingDocument"
```

---

## LLM Integration & Prompting Strategy

### Two-Stage Pipeline

```
User question
     │
     ▼
[Stage 1 — SQL Generation]
  System prompt: full schema + JOIN patterns + 3 example queries
  → LLM returns ```sql ... ``` block
     │
     ├─ SQL found? ── Execute against Neon
     │                    │
     │               Error? ── [Auto-Retry]
     │                         Feed error back to LLM → get fixed SQL → re-execute
     │
     ▼
[Stage 2 — Result Narration]
  Feed SQL + result rows → LLM returns JSON:
  { "response": "...", "highlighted_nodes": [{type, id}, ...] }
     │
     ▼
  Frontend highlights nodes in graph + shows answer
```

### Guardrails

The system prompt includes an explicit instruction:

> *"If the question is NOT about ERP, SAP, orders, deliveries, billing, payments, customers, or products in this dataset, respond EXACTLY with: 'This system is designed to answer questions related to the provided dataset only.'"*

The backend pattern-matches this string and skips SQL execution entirely.

### Prompt Engineering Techniques Used

- **Schema-in-context**: Full table + column definitions in the system prompt
- **Negative examples**: Explicitly states which tables do NOT have which columns (e.g. `outbound_delivery_headers` has no `referenceSdDocument`)
- **Few-shot SQL examples**: 3 concrete working queries in the prompt the LLM can model
- **`response_format: json_object`**: Forces valid JSON output in the narration stage
- **Low temperature (0.0–0.1)**: Reduces hallucination in SQL generation
- **Column quoting post-processor**: Programmatic safety net that double-quotes all known camelCase columns before execution, regardless of what the LLM produces

---

## Example Queries

| Question | What it does |
|---|---|
| Which products are associated with the highest number of billing documents? | GROUP BY material on billing_document_items, ranked DESC |
| Identify sales orders that were delivered but not billed | LEFT JOIN delivery→billing, WHERE billing IS NULL |
| Trace the full flow of billing document X | Multi-table chain: SO → Delivery → Billing → Journal → Payment |
| Show top 5 customers by total billed amount | SUM(totalNetAmount) GROUP BY soldToParty, JOIN business_partners |
| What is the total net amount for cancelled billing documents? | Filter billing_document_cancellations |

---

## Project Structure

```
DodgeAI/
├── backend/
│   ├── main.py              # FastAPI app, CORS
│   ├── database.py          # psycopg2 connection
│   ├── ingestion.py         # JSONL → PostgreSQL (run once)
│   ├── models.py            # Pydantic request/response models
│   ├── llm.py               # Groq client, prompts, SQL execution, auto-retry
│   ├── routers/
│   │   ├── graph.py         # GET /api/graph/*
│   │   └── chat.py          # POST /api/chat
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── App.tsx          # Split-pane layout
│   │   ├── api.ts           # fetch wrappers
│   │   ├── types.ts         # TypeScript types
│   │   └── components/
│   │       ├── GraphView/   # Cytoscape.js, lazy node expansion
│   │       ├── ChatPanel/   # Chat UI, SQL toggle, node highlights
│   │       └── NodeInspector/ # Selected node property panel
│   └── package.json
└── sap-o2c-data/            # Source JSONL files (not committed)
```

---

## Running Locally

### Prerequisites
- Python 3.11+, Node.js 18+
- [Neon](https://neon.tech) free PostgreSQL project
- [Groq](https://console.groq.com) API key (free tier)

### Backend

```bash
cd backend
cp .env.example .env
# Edit .env: fill DATABASE_URL and GROQ_API_KEY

uv venv --python 3.11
uv pip install -r requirements.txt

# Run ingestion once
.venv/Scripts/python ingestion.py

# Start server
.venv/Scripts/uvicorn main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# Open http://localhost:5173
# Backend proxy: vite.config.ts proxies /api → localhost:8000
```

---

## Deployment

### Backend → Render
1. New **Web Service** → connect repo → root directory: `backend/`
2. Runtime: **Docker**
3. Environment variables: `DATABASE_URL`, `GROQ_API_KEY`

### Frontend → Vercel
1. New project → connect repo → root directory: `frontend/`
2. Environment variable: `VITE_API_URL=https://your-render-url.onrender.com`

---

## AI Tools Used

Built with **GitHub Copilot** (Claude Sonnet 4.6). Session logs included in submission ZIP.
