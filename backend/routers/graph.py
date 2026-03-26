"""
routers/graph.py — Graph exploration endpoints.
"""
import decimal
import datetime
from fastapi import APIRouter, Depends, Query, HTTPException
from database import get_db

router = APIRouter(prefix="/api/graph", tags=["graph"])

# ─── Node type definitions ─────────────────────────────────────────────────────

NODE_TYPES = {
    "SalesOrder": {
        "table": "sales_order_headers",
        "id_col": "salesOrder",
        "label_cols": ["salesOrder", "soldToParty", "totalNetAmount", "transactionCurrency", "overallDeliveryStatus"],
        "color": "#6366f1",
    },
    "SalesOrderItem": {
        "table": "sales_order_items",
        "id_col": "salesOrder",  # composite; we use salesOrder+salesOrderItem
        "label_cols": ["salesOrder", "salesOrderItem", "material", "requestedQuantity", "netAmount"],
        "color": "#8b5cf6",
    },
    "DeliveryHeader": {
        "table": "outbound_delivery_headers",
        "id_col": "deliveryDocument",
        "label_cols": ["deliveryDocument", "overallGoodsMovementStatus", "overallPickingStatus", "creationDate"],
        "color": "#06b6d4",
    },
    "DeliveryItem": {
        "table": "outbound_delivery_items",
        "id_col": "deliveryDocument",
        "label_cols": ["deliveryDocument", "deliveryDocumentItem", "actualDeliveryQuantity", "referenceSdDocument"],
        "color": "#0891b2",
    },
    "BillingHeader": {
        "table": "billing_document_headers",
        "id_col": "billingDocument",
        "label_cols": ["billingDocument", "billingDocumentType", "totalNetAmount", "transactionCurrency", "billingDocumentIsCancelled"],
        "color": "#f59e0b",
    },
    "BillingItem": {
        "table": "billing_document_items",
        "id_col": "billingDocument",
        "label_cols": ["billingDocument", "billingDocumentItem", "material", "billingQuantity", "netAmount"],
        "color": "#d97706",
    },
    "JournalEntry": {
        "table": "journal_entry_items_ar",
        "id_col": "accountingDocument",
        "label_cols": ["accountingDocument", "accountingDocumentItem", "amountInTransactionCurrency", "postingDate", "customer"],
        "color": "#10b981",
    },
    "Payment": {
        "table": "payments_ar",
        "id_col": "accountingDocument",
        "label_cols": ["accountingDocument", "accountingDocumentItem", "amountInTransactionCurrency", "clearingDate", "customer"],
        "color": "#059669",
    },
    "Customer": {
        "table": "business_partners",
        "id_col": "businessPartner",
        "label_cols": ["businessPartner", "businessPartnerFullName", "organizationBpName1", "industry"],
        "color": "#ec4899",
    },
    "Product": {
        "table": "products",
        "id_col": "product",
        "label_cols": ["product", "productType", "productOldId", "weightUnit"],
        "color": "#f97316",
    },
    "Plant": {
        "table": "plants",
        "id_col": "plant",
        "label_cols": ["plant", "plantName", "companyCode", "country", "cityName"],
        "color": "#84cc16",
    },
}

EDGE_TYPE_DEFINITIONS = [
    {"source": "SalesOrder", "target": "SalesOrderItem", "label": "HAS_ITEM"},
    {"source": "SalesOrder", "target": "Customer", "label": "PLACED_BY"},
    {"source": "SalesOrderItem", "target": "Product", "label": "REFERENCES"},
    {"source": "SalesOrderItem", "target": "Plant", "label": "PRODUCED_AT"},
    {"source": "SalesOrderItem", "target": "DeliveryItem", "label": "FULFILLED_BY"},
    {"source": "DeliveryItem", "target": "DeliveryHeader", "label": "BELONGS_TO"},
    {"source": "DeliveryItem", "target": "BillingItem", "label": "LEADS_TO"},
    {"source": "BillingItem", "target": "BillingHeader", "label": "BELONGS_TO"},
    {"source": "BillingHeader", "target": "JournalEntry", "label": "CREATES"},
    {"source": "JournalEntry", "target": "Payment", "label": "CLEARED_BY"},
    {"source": "Customer", "target": "BillingHeader", "label": "BILLED_TO"},
]


def serialize_value(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def serialize_row(row: dict) -> dict:
    return {k: serialize_value(v) for k, v in row.items()}


@router.get("/schema")
def get_schema(conn=Depends(get_db)):
    """Return node type definitions with record counts and edge type list."""
    counts = {}
    with conn.cursor() as cur:
        for node_type, cfg in NODE_TYPES.items():
            cur.execute(f'SELECT COUNT(*) FROM {cfg["table"]}')
            counts[node_type] = cur.fetchone()[0]

    node_types = []
    for node_type, cfg in NODE_TYPES.items():
        node_types.append({
            "type": node_type,
            "count": counts[node_type],
            "color": cfg["color"],
            "table": cfg["table"],
        })

    return {
        "node_types": node_types,
        "edge_types": EDGE_TYPE_DEFINITIONS,
    }


@router.get("/nodes")
def get_nodes(
    type: str = Query(...),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    conn=Depends(get_db),
):
    """Return paginated nodes of a given entity type."""
    if type not in NODE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown type: {type}")
    cfg = NODE_TYPES[type]
    offset = (page - 1) * limit
    col_sql = ", ".join(f'"{c}"' for c in cfg["label_cols"])

    with conn.cursor() as cur:
        cur.execute(
            f'SELECT {col_sql} FROM {cfg["table"]} ORDER BY 1 LIMIT %s OFFSET %s',
            (limit, offset),
        )
        cols = [d.name for d in cur.description]
        rows = [serialize_row(dict(zip(cols, r))) for r in cur.fetchall()]

    return {"type": type, "nodes": rows, "page": page, "count": len(rows)}


@router.get("/expand")
def expand_node(
    nodeType: str = Query(...),
    nodeId: str = Query(...),
    conn=Depends(get_db),
):
    """
    Return the target node and all directly connected nodes + edges.
    Connections are derived from JOIN queries on FK relationships.
    """
    if nodeType not in NODE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown type: {nodeType}")

    nodes = []
    edges = []

    def add_node(ntype, nid, props):
        nodes.append({"type": ntype, "id": str(nid), "properties": serialize_row(props)})

    def add_edge(src_type, src_id, tgt_type, tgt_id, label):
        edges.append({
            "source": f"{src_type}:{src_id}",
            "target": f"{tgt_type}:{tgt_id}",
            "label": label,
        })

    with conn.cursor() as cur:

        def fetch_one(table, where_col, where_val, cols="*"):
            cur.execute(f'SELECT {cols} FROM {table} WHERE "{where_col}" = %s LIMIT 1', (where_val,))
            row = cur.fetchone()
            if row and cur.description:
                return dict(zip([d.name for d in cur.description], row))
            return None

        def fetch_many(table, where_col, where_val, cols="*", limit=20):
            cur.execute(
                f'SELECT {cols} FROM {table} WHERE "{where_col}" = %s LIMIT %s',
                (where_val, limit),
            )
            if not cur.description:
                return []
            col_names = [d.name for d in cur.description]
            return [dict(zip(col_names, r)) for r in cur.fetchall()]

        # ── SalesOrder ──────────────────────────────────────────────────────
        if nodeType == "SalesOrder":
            root = fetch_one("sales_order_headers", "salesOrder", nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node("SalesOrder", nodeId, root)

            # → Customer
            if root.get("soldToParty"):
                c = fetch_one("business_partners", "businessPartner", root["soldToParty"])
                if c:
                    add_node("Customer", root["soldToParty"], c)
                    add_edge("SalesOrder", nodeId, "Customer", root["soldToParty"], "PLACED_BY")

            # → SalesOrderItems
            items = fetch_many("sales_order_items", "salesOrder", nodeId)
            for item in items:
                item_id = f"{nodeId}:{item['salesOrderItem']}"
                add_node("SalesOrderItem", item_id, item)
                add_edge("SalesOrder", nodeId, "SalesOrderItem", item_id, "HAS_ITEM")

                # → Product
                if item.get("material"):
                    p = fetch_one("products", "product", item["material"])
                    if p:
                        add_node("Product", item["material"], p)
                        add_edge("SalesOrderItem", item_id, "Product", item["material"], "REFERENCES")

                # → Plant
                if item.get("productionPlant"):
                    pl = fetch_one("plants", "plant", item["productionPlant"])
                    if pl:
                        add_node("Plant", item["productionPlant"], pl)
                        add_edge("SalesOrderItem", item_id, "Plant", item["productionPlant"], "PRODUCED_AT")

        # ── Customer ────────────────────────────────────────────────────────
        elif nodeType == "Customer":
            root = fetch_one("business_partners", "businessPartner", nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node("Customer", nodeId, root)

            # → Addresses
            addrs = fetch_many("business_partner_addresses", "businessPartner", nodeId)
            for addr in addrs:
                addr_id = f"{nodeId}:{addr['addressId']}"
                add_node("Customer", addr_id, addr)

            # → Recent BillingHeaders (as sold_to_party)
            bills = fetch_many("billing_document_headers", "soldToParty", nodeId, limit=10)
            for b in bills:
                add_node("BillingHeader", b["billingDocument"], b)
                add_edge("Customer", nodeId, "BillingHeader", b["billingDocument"], "BILLED_TO")

        # ── BillingHeader ────────────────────────────────────────────────────
        elif nodeType == "BillingHeader":
            root = fetch_one("billing_document_headers", "billingDocument", nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node("BillingHeader", nodeId, root)

            # → Customer
            if root.get("soldToParty"):
                c = fetch_one("business_partners", "businessPartner", root["soldToParty"])
                if c:
                    add_node("Customer", root["soldToParty"], c)
                    add_edge("Customer", root["soldToParty"], "BillingHeader", nodeId, "BILLED_TO")

            # → BillingItems
            items = fetch_many("billing_document_items", "billingDocument", nodeId)
            for item in items:
                item_id = f"{nodeId}:{item['billingDocumentItem']}"
                add_node("BillingItem", item_id, item)
                add_edge("BillingItem", item_id, "BillingHeader", nodeId, "BELONGS_TO")

                # → DeliveryItem (via referenceSdDocument = deliveryDocument)
                if item.get("referenceSdDocument"):
                    di_rows = fetch_many("outbound_delivery_items", "deliveryDocument",
                                         item["referenceSdDocument"], limit=5)
                    for di in di_rows:
                        if di.get("deliveryDocumentItem") == item.get("referenceSdDocumentItem"):
                            di_id = f"{item['referenceSdDocument']}:{di['deliveryDocumentItem']}"
                            add_node("DeliveryItem", di_id, di)
                            add_edge("DeliveryItem", di_id, "BillingItem", item_id, "LEADS_TO")

            # → JournalEntry
            if root.get("accountingDocument"):
                je_rows = fetch_many("journal_entry_items_ar", "accountingDocument",
                                      root["accountingDocument"], limit=5)
                for je in je_rows:
                    je_id = f"{je['accountingDocument']}:{je['accountingDocumentItem']}"
                    add_node("JournalEntry", je_id, je)
                    add_edge("BillingHeader", nodeId, "JournalEntry", je_id, "CREATES")

                    # → Payment
                    if je.get("clearingAccountingDocument"):
                        pay_rows = fetch_many("payments_ar", "accountingDocument",
                                               je["clearingAccountingDocument"], limit=3)
                        for pay in pay_rows:
                            pay_id = f"{pay['accountingDocument']}:{pay['accountingDocumentItem']}"
                            add_node("Payment", pay_id, pay)
                            add_edge("JournalEntry", je_id, "Payment", pay_id, "CLEARED_BY")

        # ── DeliveryHeader ───────────────────────────────────────────────────
        elif nodeType == "DeliveryHeader":
            root = fetch_one("outbound_delivery_headers", "deliveryDocument", nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node("DeliveryHeader", nodeId, root)

            items = fetch_many("outbound_delivery_items", "deliveryDocument", nodeId)
            for item in items:
                item_id = f"{nodeId}:{item['deliveryDocumentItem']}"
                add_node("DeliveryItem", item_id, item)
                add_edge("DeliveryItem", item_id, "DeliveryHeader", nodeId, "BELONGS_TO")

                # → SalesOrderItem
                if item.get("referenceSdDocument"):
                    soi_rows = fetch_many("sales_order_items", "salesOrder",
                                          item["referenceSdDocument"], limit=5)
                    for soi in soi_rows:
                        if soi.get("salesOrderItem") == item.get("referenceSdDocumentItem"):
                            soi_id = f"{soi['salesOrder']}:{soi['salesOrderItem']}"
                            add_node("SalesOrderItem", soi_id, soi)
                            add_edge("SalesOrderItem", soi_id, "DeliveryItem", item_id, "FULFILLED_BY")

        # ── Product ──────────────────────────────────────────────────────────
        elif nodeType == "Product":
            root = fetch_one("products", "product", nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node("Product", nodeId, root)

            # description
            cur.execute(
                'SELECT * FROM product_descriptions WHERE "product" = %s AND "language" = %s LIMIT 1',
                (nodeId, "EN"),
            )
            desc_row = cur.fetchone()
            if desc_row and cur.description:
                desc = dict(zip([d.name for d in cur.description], desc_row))
                add_node("Product", f"{nodeId}:desc", desc)

            # Plants
            pp_rows = fetch_many("product_plants", "product", nodeId, limit=10)
            for pp in pp_rows:
                pl = fetch_one("plants", "plant", pp["plant"])
                if pl:
                    add_node("Plant", pp["plant"], pl)
                    add_edge("Product", nodeId, "Plant", pp["plant"], "STOCKED_IN")

        # ── JournalEntry ─────────────────────────────────────────────────────
        elif nodeType == "JournalEntry":
            # nodeId is "accountingDocument:accountingDocumentItem"
            parts = nodeId.split(":", 1)
            acc_doc = parts[0]
            root_rows = fetch_many("journal_entry_items_ar", "accountingDocument", acc_doc)
            if not root_rows:
                raise HTTPException(404, "Node not found")
            for je in root_rows:
                je_id = f"{je['accountingDocument']}:{je['accountingDocumentItem']}"
                add_node("JournalEntry", je_id, je)

                if je.get("referenceDocument"):
                    bh = fetch_one("billing_document_headers", "billingDocument", je["referenceDocument"])
                    if bh:
                        add_node("BillingHeader", je["referenceDocument"], bh)
                        add_edge("BillingHeader", je["referenceDocument"], "JournalEntry", je_id, "CREATES")

                if je.get("clearingAccountingDocument"):
                    pay_rows = fetch_many("payments_ar", "accountingDocument",
                                           je["clearingAccountingDocument"], limit=3)
                    for pay in pay_rows:
                        pay_id = f"{pay['accountingDocument']}:{pay['accountingDocumentItem']}"
                        add_node("Payment", pay_id, pay)
                        add_edge("JournalEntry", je_id, "Payment", pay_id, "CLEARED_BY")

        # ── Generic fallback ─────────────────────────────────────────────────
        else:
            cfg = NODE_TYPES[nodeType]
            root = fetch_one(cfg["table"], cfg["id_col"], nodeId)
            if not root:
                raise HTTPException(404, "Node not found")
            add_node(nodeType, nodeId, root)

    # Deduplicate nodes by (type, id)
    seen = set()
    unique_nodes = []
    for n in nodes:
        key = (n["type"], n["id"])
        if key not in seen:
            seen.add(key)
            unique_nodes.append(n)

    return {"nodes": unique_nodes, "edges": edges}
