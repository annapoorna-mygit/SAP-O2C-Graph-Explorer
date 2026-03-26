"""
ingestion.py — Create all 19 tables in Neon PostgreSQL and load SAP O2C JSONL data.
Run once:  python ingestion.py
"""
import os
import json
import glob
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
DATA_ROOT = Path(__file__).parent.parent / "sap-o2c-data"

# ─── DDL ──────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS sales_order_headers (
    "salesOrder"                    TEXT PRIMARY KEY,
    "salesOrderType"                TEXT,
    "salesOrganization"             TEXT,
    "distributionChannel"           TEXT,
    "organizationDivision"          TEXT,
    "salesGroup"                    TEXT,
    "salesOffice"                   TEXT,
    "soldToParty"                   TEXT,
    "creationDate"                  TIMESTAMPTZ,
    "createdByUser"                 TEXT,
    "lastChangeDateTime"            TIMESTAMPTZ,
    "totalNetAmount"                NUMERIC,
    "transactionCurrency"           TEXT,
    "overallDeliveryStatus"         TEXT,
    "overallOrdReltdBillgStatus"    TEXT,
    "pricingDate"                   TIMESTAMPTZ,
    "requestedDeliveryDate"         TIMESTAMPTZ,
    "headerBillingBlockReason"      TEXT,
    "deliveryBlockReason"           TEXT,
    "incotermsClassification"       TEXT,
    "incotermsLocation1"            TEXT,
    "customerPaymentTerms"          TEXT,
    "totalCreditCheckStatus"        TEXT
);

CREATE TABLE IF NOT EXISTS sales_order_items (
    "salesOrder"                    TEXT REFERENCES sales_order_headers("salesOrder"),
    "salesOrderItem"                TEXT,
    "salesOrderItemCategory"        TEXT,
    "material"                      TEXT,
    "requestedQuantity"             NUMERIC,
    "requestedQuantityUnit"         TEXT,
    "transactionCurrency"           TEXT,
    "netAmount"                     NUMERIC,
    "materialGroup"                 TEXT,
    "productionPlant"               TEXT,
    "storageLocation"               TEXT,
    "salesDocumentRjcnReason"       TEXT,
    "itemBillingBlockReason"        TEXT,
    PRIMARY KEY ("salesOrder", "salesOrderItem")
);

CREATE TABLE IF NOT EXISTS sales_order_schedule_lines (
    "salesOrder"                    TEXT,
    "salesOrderItem"                TEXT,
    "scheduleLine"                  TEXT,
    "confirmedDeliveryDate"         TIMESTAMPTZ,
    "orderQuantityUnit"             TEXT,
    "confdOrderQtyByMatlAvailCheck" NUMERIC,
    PRIMARY KEY ("salesOrder", "salesOrderItem", "scheduleLine")
);

CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
    "deliveryDocument"              TEXT PRIMARY KEY,
    "creationDate"                  TIMESTAMPTZ,
    "actualGoodsMovementDate"       TIMESTAMPTZ,
    "deliveryBlockReason"           TEXT,
    "headerBillingBlockReason"      TEXT,
    "hdrGeneralIncompletionStatus"  TEXT,
    "overallGoodsMovementStatus"    TEXT,
    "overallPickingStatus"          TEXT,
    "overallProofOfDeliveryStatus"  TEXT,
    "shippingPoint"                 TEXT
);

CREATE TABLE IF NOT EXISTS outbound_delivery_items (
    "deliveryDocument"              TEXT REFERENCES outbound_delivery_headers("deliveryDocument"),
    "deliveryDocumentItem"          TEXT,
    "actualDeliveryQuantity"        NUMERIC,
    "deliveryQuantityUnit"          TEXT,
    "batch"                         TEXT,
    "itemBillingBlockReason"        TEXT,
    "lastChangeDate"                TIMESTAMPTZ,
    "plant"                         TEXT,
    "storageLocation"               TEXT,
    "referenceSdDocument"           TEXT,
    "referenceSdDocumentItem"       TEXT,
    PRIMARY KEY ("deliveryDocument", "deliveryDocumentItem")
);

CREATE TABLE IF NOT EXISTS billing_document_headers (
    "billingDocument"               TEXT PRIMARY KEY,
    "billingDocumentType"           TEXT,
    "creationDate"                  TIMESTAMPTZ,
    "lastChangeDateTime"            TIMESTAMPTZ,
    "billingDocumentDate"           TIMESTAMPTZ,
    "billingDocumentIsCancelled"    BOOLEAN,
    "cancelledBillingDocument"      TEXT,
    "totalNetAmount"                NUMERIC,
    "transactionCurrency"           TEXT,
    "companyCode"                   TEXT,
    "fiscalYear"                    TEXT,
    "accountingDocument"            TEXT,
    "soldToParty"                   TEXT
);

CREATE TABLE IF NOT EXISTS billing_document_items (
    "billingDocument"               TEXT REFERENCES billing_document_headers("billingDocument"),
    "billingDocumentItem"           TEXT,
    "material"                      TEXT,
    "billingQuantity"               NUMERIC,
    "billingQuantityUnit"           TEXT,
    "netAmount"                     NUMERIC,
    "transactionCurrency"           TEXT,
    "referenceSdDocument"           TEXT,
    "referenceSdDocumentItem"       TEXT,
    PRIMARY KEY ("billingDocument", "billingDocumentItem")
);

CREATE TABLE IF NOT EXISTS billing_document_cancellations (
    "billingDocument"               TEXT PRIMARY KEY,
    "billingDocumentType"           TEXT,
    "creationDate"                  TIMESTAMPTZ,
    "cancelledBillingDocument"      TEXT,
    "totalNetAmount"                NUMERIC,
    "transactionCurrency"           TEXT,
    "companyCode"                   TEXT,
    "fiscalYear"                    TEXT,
    "soldToParty"                   TEXT
);

CREATE TABLE IF NOT EXISTS journal_entry_items_ar (
    "companyCode"                   TEXT,
    "fiscalYear"                    TEXT,
    "accountingDocument"            TEXT,
    "accountingDocumentItem"        TEXT,
    "glAccount"                     TEXT,
    "referenceDocument"             TEXT,
    "costCenter"                    TEXT,
    "profitCenter"                  TEXT,
    "transactionCurrency"           TEXT,
    "companyCodeCurrency"           TEXT,
    "amountInTransactionCurrency"   NUMERIC,
    "amountInCompanyCodeCurrency"   NUMERIC,
    "postingDate"                   TIMESTAMPTZ,
    "documentDate"                  TIMESTAMPTZ,
    "accountingDocumentType"        TEXT,
    "assignmentReference"           TEXT,
    "lastChangeDateTime"            TIMESTAMPTZ,
    "customer"                      TEXT,
    "financialAccountType"          TEXT,
    "clearingDate"                  TIMESTAMPTZ,
    "clearingAccountingDocument"    TEXT,
    "clearingDocFiscalYear"         TEXT,
    PRIMARY KEY ("companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem")
);

CREATE TABLE IF NOT EXISTS payments_ar (
    "companyCode"                   TEXT,
    "fiscalYear"                    TEXT,
    "accountingDocument"            TEXT,
    "accountingDocumentItem"        TEXT,
    "clearingDate"                  TIMESTAMPTZ,
    "clearingAccountingDocument"    TEXT,
    "clearingDocFiscalYear"         TEXT,
    "amountInTransactionCurrency"   NUMERIC,
    "amountInCompanyCodeCurrency"   NUMERIC,
    "transactionCurrency"           TEXT,
    "companyCodeCurrency"           TEXT,
    "customer"                      TEXT,
    "invoiceReference"              TEXT,
    "salesDocument"                 TEXT,
    "postingDate"                   TIMESTAMPTZ,
    "documentDate"                  TIMESTAMPTZ,
    "glAccount"                     TEXT,
    "financialAccountType"          TEXT,
    "profitCenter"                  TEXT,
    PRIMARY KEY ("companyCode", "fiscalYear", "accountingDocument", "accountingDocumentItem")
);

CREATE TABLE IF NOT EXISTS business_partners (
    "businessPartner"               TEXT PRIMARY KEY,
    "customer"                      TEXT,
    "businessPartnerCategory"       TEXT,
    "businessPartnerFullName"       TEXT,
    "businessPartnerName"           TEXT,
    "businessPartnerGrouping"       TEXT,
    "correspondenceLanguage"        TEXT,
    "creationDate"                  TIMESTAMPTZ,
    "industry"                      TEXT,
    "organizationBpName1"           TEXT,
    "organizationBpName2"           TEXT,
    "businessPartnerIsBlocked"      BOOLEAN,
    "isMarkedForArchiving"          BOOLEAN
);

CREATE TABLE IF NOT EXISTS business_partner_addresses (
    "businessPartner"               TEXT REFERENCES business_partners("businessPartner"),
    "addressId"                     TEXT,
    "addressTimeZone"               TEXT,
    "cityName"                      TEXT,
    "country"                       TEXT,
    "postalCode"                    TEXT,
    "region"                        TEXT,
    "streetName"                    TEXT,
    "validityStartDate"             TIMESTAMPTZ,
    "validityEndDate"               TIMESTAMPTZ,
    PRIMARY KEY ("businessPartner", "addressId")
);

CREATE TABLE IF NOT EXISTS customer_company_assignments (
    "customer"                      TEXT,
    "companyCode"                   TEXT,
    "reconciliationAccount"         TEXT,
    "paymentTerms"                  TEXT,
    "deletionIndicator"             BOOLEAN,
    "customerAccountGroup"          TEXT,
    PRIMARY KEY ("customer", "companyCode")
);

CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
    "customer"                      TEXT,
    "salesOrganization"             TEXT,
    "distributionChannel"           TEXT,
    "division"                      TEXT,
    "currency"                      TEXT,
    "customerPaymentTerms"          TEXT,
    "deliveryPriority"              TEXT,
    "incotermsClassification"       TEXT,
    "shippingCondition"             TEXT,
    "supplyingPlant"                TEXT,
    PRIMARY KEY ("customer", "salesOrganization", "distributionChannel", "division")
);

CREATE TABLE IF NOT EXISTS products (
    "product"                       TEXT PRIMARY KEY,
    "productType"                   TEXT,
    "crossPlantStatus"              TEXT,
    "creationDate"                  TIMESTAMPTZ,
    "lastChangeDate"                TIMESTAMPTZ,
    "isMarkedForDeletion"           BOOLEAN,
    "productOldId"                  TEXT,
    "grossWeight"                   NUMERIC,
    "netWeight"                     NUMERIC,
    "weightUnit"                    TEXT
);

CREATE TABLE IF NOT EXISTS product_descriptions (
    "product"                       TEXT REFERENCES products("product"),
    "language"                      TEXT,
    "productDescription"            TEXT,
    PRIMARY KEY ("product", "language")
);

CREATE TABLE IF NOT EXISTS product_plants (
    "product"                       TEXT REFERENCES products("product"),
    "plant"                         TEXT,
    "profitCenter"                  TEXT,
    "mrpType"                       TEXT,
    "availabilityCheckType"         TEXT,
    PRIMARY KEY ("product", "plant")
);

CREATE TABLE IF NOT EXISTS product_storage_locations (
    "product"                       TEXT,
    "plant"                         TEXT,
    "storageLocation"               TEXT,
    "warehouseStorageBin"           TEXT,
    PRIMARY KEY ("product", "plant", "storageLocation")
);

CREATE TABLE IF NOT EXISTS plants (
    "plant"                         TEXT PRIMARY KEY,
    "plantName"                     TEXT,
    "companyCode"                   TEXT,
    "country"                       TEXT,
    "cityName"                      TEXT,
    "language"                      TEXT
);

-- Indexes on common join columns
CREATE INDEX IF NOT EXISTS idx_soi_material       ON sales_order_items("material");
CREATE INDEX IF NOT EXISTS idx_soi_plant          ON sales_order_items("productionPlant");
CREATE INDEX IF NOT EXISTS idx_odi_ref_doc        ON outbound_delivery_items("referenceSdDocument");
CREATE INDEX IF NOT EXISTS idx_bdi_ref_doc        ON billing_document_items("referenceSdDocument");
CREATE INDEX IF NOT EXISTS idx_bdi_material       ON billing_document_items("material");
CREATE INDEX IF NOT EXISTS idx_bdh_sold_to        ON billing_document_headers("soldToParty");
CREATE INDEX IF NOT EXISTS idx_bdh_accounting     ON billing_document_headers("accountingDocument");
CREATE INDEX IF NOT EXISTS idx_jear_ref_doc       ON journal_entry_items_ar("referenceDocument");
CREATE INDEX IF NOT EXISTS idx_jear_clearing_doc  ON journal_entry_items_ar("clearingAccountingDocument");
CREATE INDEX IF NOT EXISTS idx_par_clearing_doc   ON payments_ar("clearingAccountingDocument");
CREATE INDEX IF NOT EXISTS idx_soh_sold_to        ON sales_order_headers("soldToParty");
"""

# ─── Field mappings ───────────────────────────────────────────────────────────

def _ts(val):
    """Return ISO string or None for timestamp fields."""
    if not val:
        return None
    if isinstance(val, dict):
        return None
    return str(val)[:26] if val else None

def _num(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def _bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes")

def _str(val):
    if val is None:
        return None
    return str(val).strip() or None


# ─── Loaders (table-specific field extraction) ────────────────────────────────

LOADERS = {
    "sales_order_headers": {
        "folder": "sales_order_headers",
        "table": "sales_order_headers",
        "columns": [
            "salesOrder","salesOrderType","salesOrganization","distributionChannel",
            "organizationDivision","salesGroup","salesOffice","soldToParty",
            "creationDate","createdByUser","lastChangeDateTime","totalNetAmount",
            "transactionCurrency","overallDeliveryStatus","overallOrdReltdBillgStatus",
            "pricingDate","requestedDeliveryDate","headerBillingBlockReason",
            "deliveryBlockReason","incotermsClassification","incotermsLocation1",
            "customerPaymentTerms","totalCreditCheckStatus",
        ],
        "transform": lambda r: [
            _str(r.get("salesOrder")), _str(r.get("salesOrderType")),
            _str(r.get("salesOrganization")), _str(r.get("distributionChannel")),
            _str(r.get("organizationDivision")), _str(r.get("salesGroup")),
            _str(r.get("salesOffice")), _str(r.get("soldToParty")),
            _ts(r.get("creationDate")), _str(r.get("createdByUser")),
            _ts(r.get("lastChangeDateTime")), _num(r.get("totalNetAmount")),
            _str(r.get("transactionCurrency")), _str(r.get("overallDeliveryStatus")),
            _str(r.get("overallOrdReltdBillgStatus")), _ts(r.get("pricingDate")),
            _ts(r.get("requestedDeliveryDate")), _str(r.get("headerBillingBlockReason")),
            _str(r.get("deliveryBlockReason")), _str(r.get("incotermsClassification")),
            _str(r.get("incotermsLocation1")), _str(r.get("customerPaymentTerms")),
            _str(r.get("totalCreditCheckStatus")),
        ],
    },
    "sales_order_items": {
        "folder": "sales_order_items",
        "table": "sales_order_items",
        "columns": [
            "salesOrder","salesOrderItem","salesOrderItemCategory","material",
            "requestedQuantity","requestedQuantityUnit","transactionCurrency","netAmount",
            "materialGroup","productionPlant","storageLocation","salesDocumentRjcnReason",
            "itemBillingBlockReason",
        ],
        "transform": lambda r: [
            _str(r.get("salesOrder")), _str(r.get("salesOrderItem")),
            _str(r.get("salesOrderItemCategory")), _str(r.get("material")),
            _num(r.get("requestedQuantity")), _str(r.get("requestedQuantityUnit")),
            _str(r.get("transactionCurrency")), _num(r.get("netAmount")),
            _str(r.get("materialGroup")), _str(r.get("productionPlant")),
            _str(r.get("storageLocation")), _str(r.get("salesDocumentRjcnReason")),
            _str(r.get("itemBillingBlockReason")),
        ],
    },
    "sales_order_schedule_lines": {
        "folder": "sales_order_schedule_lines",
        "table": "sales_order_schedule_lines",
        "columns": [
            "salesOrder","salesOrderItem","scheduleLine","confirmedDeliveryDate",
            "orderQuantityUnit","confdOrderQtyByMatlAvailCheck",
        ],
        "transform": lambda r: [
            _str(r.get("salesOrder")), _str(r.get("salesOrderItem")),
            _str(r.get("scheduleLine")), _ts(r.get("confirmedDeliveryDate")),
            _str(r.get("orderQuantityUnit")), _num(r.get("confdOrderQtyByMatlAvailCheck")),
        ],
    },
    "outbound_delivery_headers": {
        "folder": "outbound_delivery_headers",
        "table": "outbound_delivery_headers",
        "columns": [
            "deliveryDocument","creationDate","actualGoodsMovementDate","deliveryBlockReason",
            "headerBillingBlockReason","hdrGeneralIncompletionStatus",
            "overallGoodsMovementStatus","overallPickingStatus",
            "overallProofOfDeliveryStatus","shippingPoint",
        ],
        "transform": lambda r: [
            _str(r.get("deliveryDocument")), _ts(r.get("creationDate")),
            _ts(r.get("actualGoodsMovementDate")), _str(r.get("deliveryBlockReason")),
            _str(r.get("headerBillingBlockReason")), _str(r.get("hdrGeneralIncompletionStatus")),
            _str(r.get("overallGoodsMovementStatus")), _str(r.get("overallPickingStatus")),
            _str(r.get("overallProofOfDeliveryStatus")), _str(r.get("shippingPoint")),
        ],
    },
    "outbound_delivery_items": {
        "folder": "outbound_delivery_items",
        "table": "outbound_delivery_items",
        "columns": [
            "deliveryDocument","deliveryDocumentItem","actualDeliveryQuantity",
            "deliveryQuantityUnit","batch","itemBillingBlockReason","lastChangeDate",
            "plant","storageLocation","referenceSdDocument","referenceSdDocumentItem",
        ],
        "transform": lambda r: [
            _str(r.get("deliveryDocument")), _str(r.get("deliveryDocumentItem")),
            _num(r.get("actualDeliveryQuantity")), _str(r.get("deliveryQuantityUnit")),
            _str(r.get("batch")), _str(r.get("itemBillingBlockReason")),
            _ts(r.get("lastChangeDate")), _str(r.get("plant")),
            _str(r.get("storageLocation")), _str(r.get("referenceSdDocument")),
            _str(r.get("referenceSdDocumentItem")),
        ],
    },
    "billing_document_headers": {
        "folder": "billing_document_headers",
        "table": "billing_document_headers",
        "columns": [
            "billingDocument","billingDocumentType","creationDate","lastChangeDateTime",
            "billingDocumentDate","billingDocumentIsCancelled","cancelledBillingDocument",
            "totalNetAmount","transactionCurrency","companyCode","fiscalYear",
            "accountingDocument","soldToParty",
        ],
        "transform": lambda r: [
            _str(r.get("billingDocument")), _str(r.get("billingDocumentType")),
            _ts(r.get("creationDate")), _ts(r.get("lastChangeDateTime")),
            _ts(r.get("billingDocumentDate")), _bool(r.get("billingDocumentIsCancelled")),
            _str(r.get("cancelledBillingDocument")), _num(r.get("totalNetAmount")),
            _str(r.get("transactionCurrency")), _str(r.get("companyCode")),
            _str(r.get("fiscalYear")), _str(r.get("accountingDocument")),
            _str(r.get("soldToParty")),
        ],
    },
    "billing_document_items": {
        "folder": "billing_document_items",
        "table": "billing_document_items",
        "columns": [
            "billingDocument","billingDocumentItem","material","billingQuantity",
            "billingQuantityUnit","netAmount","transactionCurrency",
            "referenceSdDocument","referenceSdDocumentItem",
        ],
        "transform": lambda r: [
            _str(r.get("billingDocument")), _str(r.get("billingDocumentItem")),
            _str(r.get("material")), _num(r.get("billingQuantity")),
            _str(r.get("billingQuantityUnit")), _num(r.get("netAmount")),
            _str(r.get("transactionCurrency")), _str(r.get("referenceSdDocument")),
            _str(r.get("referenceSdDocumentItem")),
        ],
    },
    "billing_document_cancellations": {
        "folder": "billing_document_cancellations",
        "table": "billing_document_cancellations",
        "columns": [
            "billingDocument","billingDocumentType","creationDate","cancelledBillingDocument",
            "totalNetAmount","transactionCurrency","companyCode","fiscalYear","soldToParty",
        ],
        "transform": lambda r: [
            _str(r.get("billingDocument")), _str(r.get("billingDocumentType")),
            _ts(r.get("creationDate")), _str(r.get("cancelledBillingDocument")),
            _num(r.get("totalNetAmount")), _str(r.get("transactionCurrency")),
            _str(r.get("companyCode")), _str(r.get("fiscalYear")),
            _str(r.get("soldToParty")),
        ],
    },
    "journal_entry_items_ar": {
        "folder": "journal_entry_items_accounts_receivable",
        "table": "journal_entry_items_ar",
        "columns": [
            "companyCode","fiscalYear","accountingDocument","accountingDocumentItem",
            "glAccount","referenceDocument","costCenter","profitCenter",
            "transactionCurrency","companyCodeCurrency","amountInTransactionCurrency",
            "amountInCompanyCodeCurrency","postingDate","documentDate",
            "accountingDocumentType","assignmentReference","lastChangeDateTime",
            "customer","financialAccountType","clearingDate",
            "clearingAccountingDocument","clearingDocFiscalYear",
        ],
        "transform": lambda r: [
            _str(r.get("companyCode")), _str(r.get("fiscalYear")),
            _str(r.get("accountingDocument")), _str(r.get("accountingDocumentItem")),
            _str(r.get("glAccount")), _str(r.get("referenceDocument")),
            _str(r.get("costCenter")), _str(r.get("profitCenter")),
            _str(r.get("transactionCurrency")), _str(r.get("companyCodeCurrency")),
            _num(r.get("amountInTransactionCurrency")), _num(r.get("amountInCompanyCodeCurrency")),
            _ts(r.get("postingDate")), _ts(r.get("documentDate")),
            _str(r.get("accountingDocumentType")), _str(r.get("assignmentReference")),
            _ts(r.get("lastChangeDateTime")), _str(r.get("customer")),
            _str(r.get("financialAccountType")), _ts(r.get("clearingDate")),
            _str(r.get("clearingAccountingDocument")), _str(r.get("clearingDocFiscalYear")),
        ],
    },
    "payments_ar": {
        "folder": "payments_accounts_receivable",
        "table": "payments_ar",
        "columns": [
            "companyCode","fiscalYear","accountingDocument","accountingDocumentItem",
            "clearingDate","clearingAccountingDocument","clearingDocFiscalYear",
            "amountInTransactionCurrency","amountInCompanyCodeCurrency",
            "transactionCurrency","companyCodeCurrency","customer","invoiceReference",
            "salesDocument","postingDate","documentDate","glAccount",
            "financialAccountType","profitCenter",
        ],
        "transform": lambda r: [
            _str(r.get("companyCode")), _str(r.get("fiscalYear")),
            _str(r.get("accountingDocument")), _str(r.get("accountingDocumentItem")),
            _ts(r.get("clearingDate")), _str(r.get("clearingAccountingDocument")),
            _str(r.get("clearingDocFiscalYear")), _num(r.get("amountInTransactionCurrency")),
            _num(r.get("amountInCompanyCodeCurrency")), _str(r.get("transactionCurrency")),
            _str(r.get("companyCodeCurrency")), _str(r.get("customer")),
            _str(r.get("invoiceReference")), _str(r.get("salesDocument")),
            _ts(r.get("postingDate")), _ts(r.get("documentDate")),
            _str(r.get("glAccount")), _str(r.get("financialAccountType")),
            _str(r.get("profitCenter")),
        ],
    },
    "business_partners": {
        "folder": "business_partners",
        "table": "business_partners",
        "columns": [
            "businessPartner","customer","businessPartnerCategory","businessPartnerFullName",
            "businessPartnerName","businessPartnerGrouping","correspondenceLanguage",
            "creationDate","industry","organizationBpName1","organizationBpName2",
            "businessPartnerIsBlocked","isMarkedForArchiving",
        ],
        "transform": lambda r: [
            _str(r.get("businessPartner")), _str(r.get("customer")),
            _str(r.get("businessPartnerCategory")), _str(r.get("businessPartnerFullName")),
            _str(r.get("businessPartnerName")), _str(r.get("businessPartnerGrouping")),
            _str(r.get("correspondenceLanguage")), _ts(r.get("creationDate")),
            _str(r.get("industry")), _str(r.get("organizationBpName1")),
            _str(r.get("organizationBpName2")), _bool(r.get("businessPartnerIsBlocked")),
            _bool(r.get("isMarkedForArchiving")),
        ],
    },
    "business_partner_addresses": {
        "folder": "business_partner_addresses",
        "table": "business_partner_addresses",
        "columns": [
            "businessPartner","addressId","addressTimeZone","cityName","country",
            "postalCode","region","streetName","validityStartDate","validityEndDate",
        ],
        "transform": lambda r: [
            _str(r.get("businessPartner")), _str(r.get("addressId")),
            _str(r.get("addressTimeZone")), _str(r.get("cityName")),
            _str(r.get("country")), _str(r.get("postalCode")),
            _str(r.get("region")), _str(r.get("streetName")),
            _ts(r.get("validityStartDate")), _ts(r.get("validityEndDate")),
        ],
    },
    "customer_company_assignments": {
        "folder": "customer_company_assignments",
        "table": "customer_company_assignments",
        "columns": [
            "customer","companyCode","reconciliationAccount","paymentTerms",
            "deletionIndicator","customerAccountGroup",
        ],
        "transform": lambda r: [
            _str(r.get("customer")), _str(r.get("companyCode")),
            _str(r.get("reconciliationAccount")), _str(r.get("paymentTerms")),
            _bool(r.get("deletionIndicator")), _str(r.get("customerAccountGroup")),
        ],
    },
    "customer_sales_area_assignments": {
        "folder": "customer_sales_area_assignments",
        "table": "customer_sales_area_assignments",
        "columns": [
            "customer","salesOrganization","distributionChannel","division",
            "currency","customerPaymentTerms","deliveryPriority",
            "incotermsClassification","shippingCondition","supplyingPlant",
        ],
        "transform": lambda r: [
            _str(r.get("customer")), _str(r.get("salesOrganization")),
            _str(r.get("distributionChannel")), _str(r.get("division")),
            _str(r.get("currency")), _str(r.get("customerPaymentTerms")),
            _str(r.get("deliveryPriority")), _str(r.get("incotermsClassification")),
            _str(r.get("shippingCondition")), _str(r.get("supplyingPlant")),
        ],
    },
    "products": {
        "folder": "products",
        "table": "products",
        "columns": [
            "product","productType","crossPlantStatus","creationDate","lastChangeDate",
            "isMarkedForDeletion","productOldId","grossWeight","netWeight","weightUnit",
        ],
        "transform": lambda r: [
            _str(r.get("product")), _str(r.get("productType")),
            _str(r.get("crossPlantStatus")), _ts(r.get("creationDate")),
            _ts(r.get("lastChangeDate")), _bool(r.get("isMarkedForDeletion")),
            _str(r.get("productOldId")), _num(r.get("grossWeight")),
            _num(r.get("netWeight")), _str(r.get("weightUnit")),
        ],
    },
    "product_descriptions": {
        "folder": "product_descriptions",
        "table": "product_descriptions",
        "columns": ["product","language","productDescription"],
        "transform": lambda r: [
            _str(r.get("product")), _str(r.get("language")),
            _str(r.get("productDescription")),
        ],
    },
    "product_plants": {
        "folder": "product_plants",
        "table": "product_plants",
        "columns": [
            "product","plant","profitCenter","mrpType","availabilityCheckType",
        ],
        "transform": lambda r: [
            _str(r.get("product")), _str(r.get("plant")),
            _str(r.get("profitCenter")), _str(r.get("mrpType")),
            _str(r.get("availabilityCheckType")),
        ],
    },
    "product_storage_locations": {
        "folder": "product_storage_locations",
        "table": "product_storage_locations",
        "columns": ["product","plant","storageLocation","warehouseStorageBin"],
        "transform": lambda r: [
            _str(r.get("product")), _str(r.get("plant")),
            _str(r.get("storageLocation")), _str(r.get("warehouseStorageBin")),
        ],
    },
    "plants": {
        "folder": "plants",
        "table": "plants",
        "columns": ["plant","plantName","companyCode","country","cityName","language"],
        "transform": lambda r: [
            _str(r.get("plant")), _str(r.get("plantName")),
            _str(r.get("companyCode")), _str(r.get("country")),
            _str(r.get("cityName")), _str(r.get("language")),
        ],
    },
}

# Insertion order matters for FK constraints
LOAD_ORDER = [
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "business_partners",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
    "plants",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "journal_entry_items_ar",
    "payments_ar",
]


def load_jsonl_files(folder_name: str) -> list[dict]:
    folder = DATA_ROOT / folder_name
    records = []
    for filepath in sorted(folder.glob("*.jsonl")):
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def insert_records(conn, table: str, columns: list[str], rows: list[list]):
    if not rows:
        print(f"  [skip] {table} — no rows")
        return 0
    col_sql = ", ".join(f'"{c}"' for c in columns)
    with conn.cursor() as cur:
        execute_values(
            cur,
            f'INSERT INTO {table} ({col_sql}) VALUES %s ON CONFLICT DO NOTHING',
            rows,
            page_size=500,
        )
    conn.commit()
    return len(rows)


def main():
    print("Connecting to Neon PostgreSQL...")
    conn = psycopg2.connect(DATABASE_URL)

    print("Running DDL...")
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("  Tables and indexes created.")

    for key in LOAD_ORDER:
        cfg = LOADERS[key]
        print(f"\nLoading {key} ← {cfg['folder']}...")
        raw = load_jsonl_files(cfg["folder"])
        print(f"  {len(raw)} raw records")
        rows = []
        for r in raw:
            row = cfg["transform"](r)
            if row[0] is not None:  # skip if PK is null
                rows.append(row)
        inserted = insert_records(conn, cfg["table"], cfg["columns"], rows)
        print(f"  → {inserted} rows upserted into {cfg['table']}")

    conn.close()
    print("\nIngestion complete.")


if __name__ == "__main__":
    main()
