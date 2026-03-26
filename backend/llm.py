"""
llm.py — Groq client, prompt templates, SQL execution, and guardrail logic.
"""
import os
import re
import json
import ssl
import httpx
import psycopg2
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

# On Windows with corporate SSL inspection (Zscaler etc.), the proxy injects a
# custom CA cert that lives in the Windows system store. Use truststore to load
# it, falling back to verify=False for local dev if truststore isn't available.
try:
    import truststore
    _ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    _http_client = httpx.Client(verify=_ssl_ctx)
except Exception:
    _http_client = httpx.Client(verify=False)

_groq = Groq(api_key=os.environ["GROQ_API_KEY"], http_client=_http_client)
MODEL = "llama-3.3-70b-versatile"

# ─── Schema context for the LLM ────────────────────────────────────────────────

SCHEMA_CONTEXT = """
You have access to a SAP Order-to-Cash (O2C) PostgreSQL database with the following tables:

TABLE: sales_order_headers
  PK: "salesOrder"
  Columns: salesOrderType, salesOrganization, distributionChannel, soldToParty (FK→business_partners.businessPartner),
           creationDate, totalNetAmount, transactionCurrency, overallDeliveryStatus, overallOrdReltdBillgStatus,
           headerBillingBlockReason, deliveryBlockReason, customerPaymentTerms

TABLE: sales_order_items
  PK: (salesOrder, salesOrderItem)
  FK: salesOrder→sales_order_headers, material→products.product, productionPlant→plants.plant
  Columns: salesOrderItemCategory, material, requestedQuantity, requestedQuantityUnit,
           netAmount, transactionCurrency, materialGroup, productionPlant, storageLocation,
           salesDocumentRjcnReason, itemBillingBlockReason

TABLE: outbound_delivery_headers
  PK: "deliveryDocument"
  NOTE: Has NO referenceSdDocument column. Link to sales orders is ONLY through outbound_delivery_items.
  Columns: creationDate, actualGoodsMovementDate, deliveryBlockReason, overallGoodsMovementStatus,
           overallPickingStatus, overallProofOfDeliveryStatus, shippingPoint

TABLE: outbound_delivery_items
  PK: (deliveryDocument, deliveryDocumentItem)
  FK: deliveryDocument→outbound_delivery_headers
  Columns: actualDeliveryQuantity, deliveryQuantityUnit, batch, plant, storageLocation,
           referenceSdDocument (= the salesOrder value), referenceSdDocumentItem (= salesOrderItem value)

TABLE: billing_document_headers
  PK: "billingDocument"
  NOTE: Has NO referenceSdDocument column. Link to deliveries is ONLY through billing_document_items.
  FK: soldToParty→business_partners.businessPartner
  Columns: billingDocumentType, creationDate, billingDocumentDate, billingDocumentIsCancelled,
           cancelledBillingDocument, totalNetAmount, transactionCurrency, companyCode,
           fiscalYear, accountingDocument (FK→journal_entry_items_ar), soldToParty

TABLE: billing_document_items
  PK: (billingDocument, billingDocumentItem)
  FK: billingDocument→billing_document_headers, material→products.product
  Columns: material, billingQuantity, billingQuantityUnit, netAmount, transactionCurrency,
           referenceSdDocument (= the deliveryDocument value), referenceSdDocumentItem

TABLE: billing_document_cancellations
  PK: "billingDocument"
  Columns: billingDocumentType, creationDate, cancelledBillingDocument, totalNetAmount,
           transactionCurrency, companyCode, fiscalYear, soldToParty

TABLE: journal_entry_items_ar
  PK: (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
  FK: referenceDocument→billing_document_headers.billingDocument
  Columns: glAccount, referenceDocument (=billingDocument), costCenter, profitCenter,
           amountInTransactionCurrency, amountInCompanyCodeCurrency, postingDate,
           accountingDocumentType, customer, financialAccountType, clearingDate,
           clearingAccountingDocument (FK→payments_ar.accountingDocument)

TABLE: payments_ar
  PK: (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
  Columns: clearingDate, clearingAccountingDocument, amountInTransactionCurrency,
           amountInCompanyCodeCurrency, transactionCurrency, customer,
           invoiceReference, salesDocument, postingDate, glAccount

TABLE: business_partners
  PK: "businessPartner"  (same value as "customer" field)
  Columns: customer, businessPartnerCategory, businessPartnerFullName, businessPartnerName,
           organizationBpName1, industry, businessPartnerIsBlocked

TABLE: business_partner_addresses
  PK: (businessPartner, addressId)
  FK: businessPartner→business_partners
  Columns: cityName, country, postalCode, region, streetName

TABLE: customer_company_assignments
  PK: (customer, companyCode)
  Columns: reconciliationAccount, paymentTerms, deletionIndicator, customerAccountGroup

TABLE: customer_sales_area_assignments
  PK: (customer, salesOrganization, distributionChannel, division)
  Columns: currency, customerPaymentTerms, deliveryPriority, incotermsClassification,
           shippingCondition, supplyingPlant

TABLE: products
  PK: "product"
  Columns: productType, crossPlantStatus, creationDate, isMarkedForDeletion,
           productOldId, grossWeight, netWeight, weightUnit

TABLE: product_descriptions
  PK: (product, language)
  FK: product→products
  Columns: productDescription

TABLE: product_plants
  PK: (product, plant)
  FK: product→products, plant→plants
  Columns: profitCenter, mrpType, availabilityCheckType

TABLE: product_storage_locations
  PK: (product, plant, storageLocation)
  Columns: warehouseStorageBin

TABLE: plants
  PK: "plant"
  Columns: plantName, companyCode, country, cityName, language

═══════════════════════════════════════════════════════
CRITICAL: CORRECT JOIN PATTERNS (follow these exactly)
═══════════════════════════════════════════════════════

-- SalesOrder → Delivery (ALWAYS go through outbound_delivery_items):
  JOIN outbound_delivery_items odi ON odi."referenceSdDocument" = soh."salesOrder"
  JOIN outbound_delivery_headers odh ON odh."deliveryDocument" = odi."deliveryDocument"

-- Delivery → Billing (ALWAYS go through billing_document_items):
  JOIN billing_document_items bdi ON bdi."referenceSdDocument" = odi."deliveryDocument"
  JOIN billing_document_headers bdh ON bdh."billingDocument" = bdi."billingDocument"

-- Billing → Journal Entry:
  JOIN journal_entry_items_ar je ON je."accountingDocument" = bdh."accountingDocument"

-- Journal Entry → Payment:
  JOIN payments_ar pay ON pay."accountingDocument" = je."clearingAccountingDocument"

-- SalesOrder → Customer name:
  JOIN business_partners bp ON bp."businessPartner" = soh."soldToParty"

-- Product → Description:
  LEFT JOIN product_descriptions pd ON pd."product" = bdi."material" AND pd."language" = 'EN'

═══════════════════════════════════════════════════════
EXAMPLE QUERIES
═══════════════════════════════════════════════════════

-- Q: Sales orders delivered but not billed:
SELECT DISTINCT soh."salesOrder", soh."soldToParty", soh."totalNetAmount"
FROM sales_order_headers soh
JOIN outbound_delivery_items odi ON odi."referenceSdDocument" = soh."salesOrder"
LEFT JOIN billing_document_items bdi ON bdi."referenceSdDocument" = odi."deliveryDocument"
WHERE bdi."billingDocument" IS NULL
LIMIT 50;

-- Q: Products with highest number of billing documents:
SELECT bdi."material",
       COALESCE(pd."productDescription", bdi."material") AS product_name,
       COUNT(DISTINCT bdi."billingDocument") AS num_billing_docs
FROM billing_document_items bdi
LEFT JOIN product_descriptions pd ON pd."product" = bdi."material" AND pd."language" = 'EN'
GROUP BY bdi."material", pd."productDescription"
ORDER BY num_billing_docs DESC
LIMIT 20;

-- Q: Full O2C trace for a billing document:
SELECT soh."salesOrder", odi."deliveryDocument", bdh."billingDocument",
       je."accountingDocument", pay."accountingDocument" AS payment_doc
FROM billing_document_headers bdh
JOIN billing_document_items bdi ON bdi."billingDocument" = bdh."billingDocument"
JOIN outbound_delivery_items odi ON odi."deliveryDocument" = bdi."referenceSdDocument"
JOIN sales_order_headers soh ON soh."salesOrder" = odi."referenceSdDocument"
LEFT JOIN journal_entry_items_ar je ON je."accountingDocument" = bdh."accountingDocument"
LEFT JOIN payments_ar pay ON pay."accountingDocument" = je."clearingAccountingDocument"
WHERE bdh."billingDocument" = '90073895'
LIMIT 50;
"""

SYSTEM_PROMPT = f"""You are a SAP O2C (Order-to-Cash) data analyst assistant.
You ONLY answer questions about the ERP dataset described below.

STRICT GUARDRAIL: If the user's question is NOT about ERP, SAP, orders, deliveries, billing,
payments, customers, products, plants, or any data in this dataset, you MUST respond with
EXACTLY this text and nothing else:
"This system is designed to answer questions related to the provided dataset only."

For relevant questions, your job is to:
1. Write a valid PostgreSQL SQL query to answer the question.
2. Wrap the SQL in a ```sql ... ``` code block.
3. After the SQL block, briefly explain what the query does in 1-2 sentences.
4. CRITICAL: ALL column names in this database are camelCase (e.g. salesOrder, soldToParty).
   PostgreSQL folds unquoted identifiers to lowercase, so you MUST double-quote EVERY column
   name AND table name without exception. Examples:
     CORRECT:   SELECT "salesOrder", "soldToParty" FROM sales_order_headers
     INCORRECT: SELECT salesOrder, soldToParty FROM sales_order_headers
   Failing to quote will cause "column does not exist" errors.
5. Limit results to 50 rows unless the user asks for more.
6. For product names, JOIN with product_descriptions pd ON pd."product" = bdi."material" AND pd."language" = 'EN'.
7. For customer names, JOIN with business_partners bp ON bp."businessPartner" = soh."soldToParty".

{SCHEMA_CONTEXT}
"""

FIX_SQL_SYSTEM = """You are a PostgreSQL expert. A SQL query failed with an error.
Fix the query and return ONLY the corrected SQL wrapped in a ```sql ... ``` block.
The most common cause of errors in this schema is unquoted camelCase column names.
ALL column names MUST be double-quoted (e.g. "salesOrder", "billingDocument").
Table names do NOT need quoting. Do not change the query logic, only fix syntax issues."""

NARRATION_SYSTEM = """You are a concise SAP O2C data analyst.
Given a SQL query and its result rows, answer the user's original question in 2-4 clear sentences.
Be specific: include numbers, names, and IDs from the data.
If the result set is empty, say so clearly.
Also, identify the most relevant entity IDs from the results to highlight in the graph.
Return your answer as JSON with this exact shape:
{{
  "response": "..natural language answer..",
  "highlighted_nodes": [
    {{"type": "SalesOrder", "id": "740506"}},
    ...
  ]
}}
Valid types: SalesOrder, DeliveryHeader, BillingHeader, JournalEntry, Payment, Customer, Product, Plant
Only include the most relevant 1-10 nodes. If no specific entities are relevant, use an empty array.
"""

GUARDRAIL_MARKER = "This system is designed to answer questions related to the provided dataset only."


def is_guardrailed(text: str) -> bool:
    return GUARDRAIL_MARKER.lower() in text.lower()


def extract_sql(text: str) -> str | None:
    match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def fix_sql(bad_sql: str, error: str) -> str | None:
    """Ask the LLM to fix a broken SQL query given the Postgres error message."""
    prompt = f"Original SQL:\n```sql\n{bad_sql}\n```\n\nPostgreSQL error:\n{error}"
    resp = _groq.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": FIX_SQL_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
        max_tokens=512,
    )
    text = resp.choices[0].message.content or ""
    return extract_sql(text)


def generate_sql(question: str, history: list[dict]) -> tuple[str, str | None]:
    """
    Returns (llm_text, sql_or_None).
    llm_text is the raw LLM response.
    sql_or_None is the extracted SQL if found.
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history[-6:]:  # keep last 3 turns for context
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": question})

    resp = _groq.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=1024,
    )
    text = resp.choices[0].message.content or ""
    if is_guardrailed(text):
        return text, None
    sql = extract_sql(text)
    return text, sql


def _quote_columns(sql: str) -> str:
    """
    Post-process SQL to double-quote any unquoted camelCase identifiers.
    Catches cases where the LLM forgets to quote column names.
    """
    # Match identifiers that contain uppercase letters (camelCase) that aren't already quoted
    # and aren't SQL keywords. Simple but effective for this schema.
    camel_cols = [
        'salesOrder', 'salesOrderItem', 'salesOrderType', 'salesOrganization',
        'distributionChannel', 'organizationDivision', 'salesGroup', 'salesOffice',
        'soldToParty', 'creationDate', 'createdByUser', 'lastChangeDateTime', 'lastChangeDate',
        'totalNetAmount', 'transactionCurrency', 'overallDeliveryStatus',
        'overallOrdReltdBillgStatus', 'pricingDate', 'requestedDeliveryDate',
        'headerBillingBlockReason', 'deliveryBlockReason', 'customerPaymentTerms',
        'totalCreditCheckStatus', 'salesOrderItemCategory', 'requestedQuantity',
        'requestedQuantityUnit', 'netAmount', 'materialGroup', 'productionPlant',
        'storageLocation', 'salesDocumentRjcnReason', 'itemBillingBlockReason',
        'scheduleLine', 'confirmedDeliveryDate', 'orderQuantityUnit',
        'confdOrderQtyByMatlAvailCheck', 'deliveryDocument', 'deliveryDocumentItem',
        'actualGoodsMovementDate', 'hdrGeneralIncompletionStatus',
        'overallGoodsMovementStatus', 'overallPickingStatus',
        'overallProofOfDeliveryStatus', 'shippingPoint', 'actualDeliveryQuantity',
        'deliveryQuantityUnit', 'referenceSdDocument', 'referenceSdDocumentItem',
        'billingDocument', 'billingDocumentItem', 'billingDocumentType',
        'billingDocumentDate', 'billingDocumentIsCancelled', 'cancelledBillingDocument',
        'companyCode', 'fiscalYear', 'accountingDocument', 'accountingDocumentItem',
        'accountingDocumentType', 'glAccount', 'referenceDocument', 'costCenter',
        'profitCenter', 'companyCodeCurrency', 'amountInTransactionCurrency',
        'amountInCompanyCodeCurrency', 'postingDate', 'documentDate',
        'assignmentReference', 'financialAccountType', 'clearingDate',
        'clearingAccountingDocument', 'clearingDocFiscalYear', 'invoiceReference',
        'salesDocument', 'businessPartner', 'businessPartnerCategory',
        'businessPartnerFullName', 'businessPartnerName', 'businessPartnerGrouping',
        'correspondenceLanguage', 'organizationBpName1', 'organizationBpName2',
        'businessPartnerIsBlocked', 'isMarkedForArchiving', 'addressId',
        'addressTimeZone', 'cityName', 'postalCode', 'validityStartDate',
        'validityEndDate', 'reconciliationAccount', 'paymentTerms',
        'deletionIndicator', 'customerAccountGroup', 'deliveryPriority',
        'incotermsClassification', 'shippingCondition', 'supplyingPlant',
        'productType', 'crossPlantStatus', 'isMarkedForDeletion', 'productOldId',
        'grossWeight', 'netWeight', 'weightUnit', 'productDescription',
        'mrpType', 'availabilityCheckType', 'warehouseStorageBin',
        'plantName', 'billingQuantity', 'billingQuantityUnit',
        'customer', 'material', 'product', 'plant', 'batch', 'region',
        'streetName', 'country', 'language', 'industry',
    ]
    for col in camel_cols:
        # Replace unquoted occurrences: word boundary, not preceded or followed by "
        sql = re.sub(
            r'(?<!")'  + r'\b' + re.escape(col) + r'\b' + r'(?!")',
            f'"{col}"',
            sql,
        )
    # Clean up any double-double-quoting that may have resulted (""col"")
    sql = re.sub(r'""([^"]+)""', r'"\1"', sql)
    return sql


def execute_sql(conn, sql: str) -> tuple[list[dict], str | None]:
    """Run SQL and return (rows_as_dicts, error_or_None)."""
    sql = _quote_columns(sql)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return [], None
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchmany(100)]
            return rows, None
    except Exception as e:
        return [], str(e)


def narrate_results(question: str, sql: str, rows: list[dict]) -> dict:
    """Produce human-readable answer + highlighted_nodes from query results."""
    rows_preview = rows[:30]

    # Serialize rows — handle non-JSON-serializable types (dates, Decimal)
    def default_serializer(obj):
        import decimal, datetime
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return str(obj)

    rows_json = json.dumps(rows_preview, default=default_serializer, ensure_ascii=False)

    prompt = (
        f"User question: {question}\n\n"
        f"SQL executed:\n{sql}\n\n"
        f"Result rows ({len(rows_preview)} of {len(rows)} total):\n{rows_json}"
    )

    resp = _groq.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": NARRATION_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=512,
        response_format={"type": "json_object"},
    )
    text = resp.choices[0].message.content or "{}"
    try:
        result = json.loads(text)
        if "response" not in result:
            result["response"] = text
        if "highlighted_nodes" not in result:
            result["highlighted_nodes"] = []
        return result
    except json.JSONDecodeError:
        return {"response": text, "highlighted_nodes": []}
