"""
routers/chat.py — LLM-powered conversational query endpoint.
"""
from fastapi import APIRouter, Depends
from database import get_db
from models import ChatRequest, ChatResponse, HighlightedNode
from llm import generate_sql, execute_sql, narrate_results, fix_sql, GUARDRAIL_MARKER

router = APIRouter(prefix="/api/chat", tags=["chat"])


@router.post("", response_model=ChatResponse)
def chat(req: ChatRequest, conn=Depends(get_db)):
    history = [{"role": m.role, "content": m.content} for m in req.history]

    # Step 1: Guard check + SQL generation
    llm_text, sql = generate_sql(req.message, history)

    # Guardrail fired → return as-is
    if sql is None:
        return ChatResponse(
            response=GUARDRAIL_MARKER if GUARDRAIL_MARKER.lower() in llm_text.lower()
                     else llm_text,
            sql=None,
            highlighted_nodes=[],
        )

    # Step 2: Execute SQL — with one automatic retry on failure
    rows, error = execute_sql(conn, sql)
    if error:
        fixed_sql = fix_sql(sql, error)
        if fixed_sql:
            rows, error = execute_sql(conn, fixed_sql)
            sql = fixed_sql  # use the fixed version in the response

    if error:
        # Both attempts failed — return a friendly message
        return ChatResponse(
            response="I couldn't retrieve that data. The query may reference columns or values not present in this dataset. Try rephrasing your question.",
            sql=sql,
            highlighted_nodes=[],
            error=error,
        )

    # Step 3: Narrate results
    result = narrate_results(req.message, sql, rows)

    highlighted = [
        HighlightedNode(type=n["type"], id=n["id"])
        for n in result.get("highlighted_nodes", [])
        if isinstance(n, dict) and "type" in n and "id" in n
    ]

    return ChatResponse(
        response=result.get("response", ""),
        sql=sql,
        highlighted_nodes=highlighted,
    )
