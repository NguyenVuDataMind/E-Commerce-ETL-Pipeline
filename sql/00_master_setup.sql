-- =================================================================
-- MASTER DATABASE SETUP SCRIPT
-- This single file handles the entire database creation process.
-- =================================================================
PRINT '--- Starting Master Database Setup ---';
GO

-- === STEP 1: CREATE DATABASE ===
PRINT 'Step 1: Creating database Facolos_Staging...';
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'Facolos_Staging')
BEGIN
  CREATE DATABASE [Facolos_Staging];
  PRINT 'Database Facolos_Staging created.';
END
ELSE
BEGIN
  PRINT 'Database Facolos_Staging already exists.';
END
GO

-- === STEP 2: SWITCH TO THE NEW DATABASE CONTEXT ===
PRINT 'Step 2: Switching to database Facolos_Staging...';
USE Facolos_Staging;
GO

-- === STEP 2.1: DROP OLD SCHEMAS AND TABLES ===
PRINT 'Step 2.1: Cleaning up old schemas and tables...';

-- Drop old tables first (to avoid FK constraints)
IF OBJECT_ID('Facolos_ETL_Control.api_token_storage', 'U') IS NOT NULL DROP TABLE Facolos_ETL_Control.api_token_storage;
IF OBJECT_ID('Facolos_ETL_Control.batch_runs', 'U') IS NOT NULL DROP TABLE Facolos_ETL_Control.batch_runs;
IF OBJECT_ID('Facolos_ETL_Control.data_sources', 'U') IS NOT NULL DROP TABLE Facolos_ETL_Control.data_sources;
IF OBJECT_ID('Facolos_MISA_CRM.customers', 'U') IS NOT NULL DROP TABLE Facolos_MISA_CRM.customers;
IF OBJECT_ID('Facolos_MISA_CRM.products', 'U') IS NOT NULL DROP TABLE Facolos_MISA_CRM.products;
IF OBJECT_ID('Facolos_MISA_CRM.contacts', 'U') IS NOT NULL DROP TABLE Facolos_MISA_CRM.contacts;
IF OBJECT_ID('Facolos_MISA_CRM.sale_orders_flattened', 'U') IS NOT NULL DROP TABLE Facolos_MISA_CRM.sale_orders_flattened;
IF OBJECT_ID('Facolos_MISA_CRM.stocks', 'U') IS NOT NULL DROP TABLE Facolos_MISA_CRM.stocks;

-- Drop old schemas
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_TikTok_Shop') DROP SCHEMA Facolos_TikTok_Shop;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Shopee') DROP SCHEMA Facolos_Shopee;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Lazada') DROP SCHEMA Facolos_Lazada;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Sendo') DROP SCHEMA Facolos_Sendo;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_TikTok_Ads') DROP SCHEMA Facolos_TikTok_Ads;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Facebook_Ads') DROP SCHEMA Facolos_Facebook_Ads;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Google_Ads') DROP SCHEMA Facolos_Google_Ads;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Google_Analytics') DROP SCHEMA Facolos_Google_Analytics;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Facebook_Pixel') DROP SCHEMA Facolos_Facebook_Pixel;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_MISA_CRM') DROP SCHEMA Facolos_MISA_CRM;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Internal_ERP') DROP SCHEMA Facolos_Internal_ERP;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_Data_Mart') DROP SCHEMA Facolos_Data_Mart;
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'Facolos_ETL_Control') DROP SCHEMA Facolos_ETL_Control;

PRINT 'Old schemas and tables cleaned up successfully!';
GO

-- === STEP 3: CREATE ESSENTIAL SCHEMAS ===
PRINT 'Step 3: Creating essential schemas...';
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl_control') EXEC('CREATE SCHEMA etl_control');
GO
IF OBJECT_ID('etl_control.batch_runs', 'U') IS NOT NULL DROP TABLE etl_control.batch_runs;
GO
CREATE TABLE etl_control.batch_runs (
    batch_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    data_source NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    start_time DATETIME2 DEFAULT GETUTCDATE(),
    end_time DATETIME2,
    status NVARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    records_extracted INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    error_message NVARCHAR(MAX),
    created_by NVARCHAR(50) DEFAULT 'facolos_etl_system'
);
GO
IF OBJECT_ID('etl_control.data_sources', 'U') IS NOT NULL DROP TABLE etl_control.data_sources;
GO
CREATE TABLE etl_control.data_sources (
    source_id NVARCHAR(50) PRIMARY KEY,
    source_name NVARCHAR(100) NOT NULL,
    source_type NVARCHAR(50) NOT NULL,
    is_active BIT DEFAULT 1,
    last_extract_time DATETIME2,
    extract_frequency_hours INT DEFAULT 6,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    updated_at DATETIME2 DEFAULT GETUTCDATE()
);
GO
INSERT INTO etl_control.data_sources (source_id, source_name, source_type, extract_frequency_hours) VALUES 
('tiktok_shop', 'TikTok Shop', 'ecommerce', 6),
('misa_crm', 'MISA CRM', 'crm', 24),
('shopee', 'Shopee', 'ecommerce', 6); -- Future Stage 2
GO
PRINT 'Enterprise schemas created successfully!';
GO



-- === STEP 4: CREATE MISA CRM STAGING TABLES ===
PRINT 'Step 4: Creating MISA CRM staging tables...';
GO

-- Drop existing MISA CRM tables if they exist for a clean setup
IF OBJECT_ID('staging.misa_sale_orders_flattened', 'U') IS NOT NULL DROP TABLE staging.misa_sale_orders_flattened;
IF OBJECT_ID('staging.misa_customers', 'U') IS NOT NULL DROP TABLE staging.misa_customers;
IF OBJECT_ID('staging.misa_contacts', 'U') IS NOT NULL DROP TABLE staging.misa_contacts;
IF OBJECT_ID('staging.misa_stocks', 'U') IS NOT NULL DROP TABLE staging.misa_stocks;
IF OBJECT_ID('staging.misa_products', 'U') IS NOT NULL DROP TABLE staging.misa_products;
GO

-- 1. MISA CUSTOMERS
PRINT 'Creating table staging.misa_customers...';
CREATE TABLE staging.misa_customers (
    id BIGINT PRIMARY KEY,
    account_number NVARCHAR(50),
    account_code NVARCHAR(50),
    account_name NVARCHAR(500),
    account_short_name NVARCHAR(255),
    owner_name NVARCHAR(255),
    office_tel NVARCHAR(50),
    office_email NVARCHAR(255),
    website NVARCHAR(500),
    fax NVARCHAR(50),
    billing_address NVARCHAR(MAX),
    billing_country NVARCHAR(100),
    billing_province NVARCHAR(100),
    billing_district NVARCHAR(100),
    billing_ward NVARCHAR(100),
    billing_street NVARCHAR(255),
    billing_code NVARCHAR(20),
    shipping_address NVARCHAR(MAX),
    shipping_country NVARCHAR(100),
    shipping_province NVARCHAR(100),
    shipping_district NVARCHAR(100),
    shipping_ward NVARCHAR(100),
    shipping_street NVARCHAR(255),
    shipping_code NVARCHAR(20),
    business_type NVARCHAR(100),
    industry NVARCHAR(100),
    annual_revenue DECIMAL(15,2),
    tax_code NVARCHAR(50),
    bank_account NVARCHAR(100),
    bank_name NVARCHAR(255),
    debt DECIMAL(15,2),
    debt_limit DECIMAL(15,2),
    number_of_days_owed DECIMAL(10,2),
    number_orders DECIMAL(10,2),
    order_sales DECIMAL(15,2),
    average_order_value DECIMAL(15,2),
    average_number_of_days_between_purchases DECIMAL(10,2),
    number_days_without_purchase DECIMAL(10,2),
    list_product_category NVARCHAR(MAX),
    list_product NVARCHAR(MAX),
    purchase_date_recent DATETIME2,
    purchase_date_first DATETIME2,
    customer_since_date DATETIME2,
    last_interaction_date DATETIME2,
    last_visit_date DATETIME2,
    last_call_date DATETIME2,
    is_personal BIT DEFAULT 0,
    gender NVARCHAR(20),
    identification NVARCHAR(50),
    issued_on DATETIME2,
    place_of_issue NVARCHAR(255),
    celebrate_date DATETIME2,
    organization_unit_name NVARCHAR(255),
    form_layout NVARCHAR(100),
    rating NVARCHAR(50),
    lead_source NVARCHAR(100),
    sector_name NVARCHAR(100),
    no_of_employee_name NVARCHAR(100),
    parent_account_name NVARCHAR(255),
    account_type NVARCHAR(100),
    inactive BIT DEFAULT 0,
    is_public BIT DEFAULT 0,
    is_distributor BIT DEFAULT 0,
    is_portal_access BIT DEFAULT 0,
    portal_username NVARCHAR(100),
    billing_long DECIMAL(10,7),
    billing_lat DECIMAL(10,7),
    shipping_long DECIMAL(10,7),
    shipping_lat DECIMAL(10,7),
    custom_field13 NVARCHAR(MAX),
    custom_field14 NVARCHAR(MAX),
    description NVARCHAR(MAX),
    tag NVARCHAR(MAX),
    budget_code NVARCHAR(50),
    total_score DECIMAL(10,2),
    number_days_not_interacted NVARCHAR(50),
    related_users NVARCHAR(MAX),
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),
    last_modified_date DATETIME2,
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO
CREATE INDEX IX_misa_customers_account_number ON staging.misa_customers(account_number);
CREATE INDEX IX_misa_customers_account_code ON staging.misa_customers(account_code);
CREATE INDEX IX_misa_customers_modified_date ON staging.misa_customers(modified_date);
GO

-- 2. MISA SALE ORDERS FLATTENED
PRINT 'Creating table staging.misa_sale_orders_flattened...';
CREATE TABLE staging.misa_sale_orders_flattened (
    order_id BIGINT NOT NULL,
    order_sale_order_no NVARCHAR(50),
    order_account_name NVARCHAR(500),
    order_sale_order_name NVARCHAR(MAX),
    order_sale_order_amount DECIMAL(15,2),
    order_sale_order_date DATETIME2,
    order_due_date DATETIME2,
    order_status NVARCHAR(50),
    order_delivery_status NVARCHAR(50),
    order_pay_status NVARCHAR(50),
    order_revenue_status NVARCHAR(50),
    order_total_summary DECIMAL(15,2),
    order_tax_summary DECIMAL(15,2),
    order_discount_summary DECIMAL(15,2),
    order_to_currency_summary DECIMAL(15,2),
    order_total_receipted_amount DECIMAL(15,2),
    order_balance_receipt_amount DECIMAL(15,2),
    order_invoiced_amount DECIMAL(15,2),
    order_un_invoiced_amount DECIMAL(15,2),
    order_currency_type NVARCHAR(10),
    order_exchange_rate DECIMAL(10,4),
    order_is_use_currency BIT,
    order_billing_address NVARCHAR(MAX),
    order_billing_country NVARCHAR(100),
    order_billing_province NVARCHAR(100),
    order_billing_district NVARCHAR(100),
    order_billing_ward NVARCHAR(100),
    order_billing_street NVARCHAR(255),
    order_billing_code NVARCHAR(20),
    order_shipping_address NVARCHAR(MAX),
    order_shipping_country NVARCHAR(100),
    order_shipping_province NVARCHAR(100),
    order_shipping_district NVARCHAR(100),
    order_shipping_ward NVARCHAR(100),
    order_shipping_street NVARCHAR(255),
    order_shipping_code NVARCHAR(20),
    order_phone NVARCHAR(50),
    order_billing_contact NVARCHAR(255),
    order_shipping_contact_name NVARCHAR(255),
    order_organization_unit_name NVARCHAR(255),
    order_owner_name NVARCHAR(255),
    order_employee_code NVARCHAR(50),
    order_account_code NVARCHAR(50),
    order_contact_code NVARCHAR(50),
    order_book_date DATETIME2,
    order_deadline_date DATETIME2,
    order_delivery_date DATETIME2,
    order_paid_date DATETIME2,
    order_invoice_date DATETIME2,
    order_production_date DATETIME2,
    item_id BIGINT NOT NULL,
    item_product_code NVARCHAR(50),
    item_unit NVARCHAR(20),
    item_usage_unit NVARCHAR(20),
    item_price DECIMAL(15,2),
    item_amount DECIMAL(10,2),
    item_usage_unit_amount DECIMAL(10,2),
    item_usage_unit_price DECIMAL(15,2),
    item_total DECIMAL(15,2),
    item_to_currency DECIMAL(15,2),
    item_discount DECIMAL(15,2),
    item_tax DECIMAL(15,2),
    item_tax_percent NVARCHAR(10),
    item_discount_percent DECIMAL(5,2),
    item_price_after_tax DECIMAL(15,2),
    item_price_after_discount DECIMAL(15,2),
    item_to_currency_after_discount DECIMAL(15,2),
    item_description NVARCHAR(MAX),
    item_description_product NVARCHAR(MAX),
    item_stock_name NVARCHAR(255),
    item_batch_number NVARCHAR(100),
    item_serial_number NVARCHAR(100),
    item_expire_date DATETIME2,
    item_height DECIMAL(10,2),
    item_width DECIMAL(10,2),
    item_length DECIMAL(10,2),
    item_radius DECIMAL(10,2),
    item_mass DECIMAL(10,2),
    item_exist_amount DECIMAL(10,2),
    item_shipping_amount DECIMAL(10,2),
    item_sort_order INT,
    item_ratio DECIMAL(10,4),
    item_operator NVARCHAR(20),
    item_promotion NVARCHAR(MAX),
    item_is_promotion BIT,
    item_custom_field1 DECIMAL(15,2),
    item_produced_quantity DECIMAL(10,2),
    item_quantity_ordered DECIMAL(10,2),
    item_sale_order_product NVARCHAR(MAX),
    has_multiple_items BIT DEFAULT 0,
    total_items_in_order INT DEFAULT 0,
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api',
    CONSTRAINT PK_misa_sale_orders_flattened PRIMARY KEY (order_id, item_id)
);
GO
CREATE INDEX IX_misa_sale_orders_order_id ON staging.misa_sale_orders_flattened(order_id);
CREATE INDEX IX_misa_sale_orders_item_id ON staging.misa_sale_orders_flattened(item_id);
CREATE INDEX IX_misa_sale_orders_order_date ON staging.misa_sale_orders_flattened(order_sale_order_date);
GO

-- 3. MISA CONTACTS
PRINT 'Creating table staging.misa_contacts...';
CREATE TABLE staging.misa_contacts (
    id BIGINT PRIMARY KEY,
    contact_code NVARCHAR(50),
    account_code NVARCHAR(50),
    contact_name NVARCHAR(255),
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    salutation NVARCHAR(20),
    mobile NVARCHAR(50),
    office_tel NVARCHAR(50),
    other_phone NVARCHAR(50),
    office_email NVARCHAR(255),
    email NVARCHAR(255),
    facebook NVARCHAR(255),
    zalo NVARCHAR(100),
    account_name NVARCHAR(500),
    title NVARCHAR(100),
    department NVARCHAR(100),
    account_type NVARCHAR(100),
    mailing_address NVARCHAR(MAX),
    mailing_country NVARCHAR(100),
    mailing_province NVARCHAR(100),
    mailing_district NVARCHAR(100),
    mailing_ward NVARCHAR(100),
    mailing_street NVARCHAR(255),
    mailing_zip NVARCHAR(20),
    shipping_address NVARCHAR(MAX),
    shipping_country NVARCHAR(100),
    shipping_province NVARCHAR(100),
    shipping_district NVARCHAR(100),
    shipping_ward NVARCHAR(100),
    shipping_street NVARCHAR(255),
    shipping_zip NVARCHAR(20),
    mailing_long DECIMAL(10,7),
    mailing_lat DECIMAL(10,7),
    shipping_long DECIMAL(10,7),
    shipping_lat DECIMAL(10,7),
    date_of_birth DATETIME2,
    gender NVARCHAR(20),
    married_status NVARCHAR(50),
    bank_account NVARCHAR(100),
    bank_name NVARCHAR(255),
    email_opt_out BIT DEFAULT 0,
    phone_opt_out BIT DEFAULT 0,
    lead_source NVARCHAR(100),
    customer_since_date DATETIME2,
    organization_unit_name NVARCHAR(255),
    owner_name NVARCHAR(255),
    form_layout NVARCHAR(100),
    inactive BIT DEFAULT 0,
    total_score DECIMAL(10,2),
    last_interaction_date DATETIME2,
    last_visit_date DATETIME2,
    last_call_date DATETIME2,
    number_days_not_interacted DECIMAL(10,2),
    is_public BIT DEFAULT 0,
    tag NVARCHAR(MAX),
    related_users NVARCHAR(MAX),
    description NVARCHAR(MAX),
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO
CREATE INDEX IX_misa_contacts_account_code ON staging.misa_contacts(account_code);
CREATE INDEX IX_misa_contacts_contact_code ON staging.misa_contacts(contact_code);
GO

-- 4. MISA STOCKS
PRINT 'Creating table staging.misa_stocks...';
CREATE TABLE staging.misa_stocks (
    stock_code NVARCHAR(50) PRIMARY KEY,
    act_database_id UNIQUEIDENTIFIER,
    async_id UNIQUEIDENTIFIER,
    stock_name NVARCHAR(255),
    description NVARCHAR(MAX),
    inactive BIT DEFAULT 0,
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO
CREATE INDEX IX_misa_stocks_stock_code ON staging.misa_stocks(stock_code);
GO

-- 5. MISA PRODUCTS
PRINT 'Creating table staging.misa_products...';
CREATE TABLE staging.misa_products (
    id BIGINT PRIMARY KEY,
    product_code NVARCHAR(50) UNIQUE,
    product_name NVARCHAR(500),
    product_category NVARCHAR(100),
    usage_unit NVARCHAR(50),
    description NVARCHAR(MAX),
    sale_description NVARCHAR(MAX),
    unit_price DECIMAL(15,2),
    purchased_price DECIMAL(15,2),
    unit_cost DECIMAL(15,2),
    unit_price1 DECIMAL(15,2),
    unit_price2 DECIMAL(15,2),
    unit_price_fixed DECIMAL(15,2),
    price_after_tax BIT DEFAULT 0,
    tax NVARCHAR(50),
    is_use_tax BIT DEFAULT 0,
    product_properties NVARCHAR(100),
    is_follow_serial_number BIT DEFAULT 0,
    is_set_product BIT DEFAULT 0,
    quantity_formula NVARCHAR(MAX),
    default_stock NVARCHAR(100),
    warranty_period NVARCHAR(100),
    warranty_description NVARCHAR(MAX),
    organization_unit_name NVARCHAR(255),
    owner_name NVARCHAR(255),
    form_layout NVARCHAR(100),
    source NVARCHAR(100),
    inactive BIT DEFAULT 0,
    is_public BIT DEFAULT 0,
    avatar NVARCHAR(MAX),
    tag NVARCHAR(MAX),
    created_date DATETIME2,
    created_by NVARCHAR(255),
    modified_date DATETIME2,
    modified_by NVARCHAR(255),
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'misa_crm_api'
);
GO
CREATE INDEX IX_misa_products_product_code ON staging.misa_products(product_code);
GO

-- === STEP 5: CREATE TIKTOK SHOP STAGING TABLE ===
PRINT 'Step 5: Creating table staging.tiktok_shop_order_detail...';
GO

IF OBJECT_ID('staging.tiktok_shop_order_detail', 'U') IS NOT NULL DROP TABLE staging.tiktok_shop_order_detail;
GO

CREATE TABLE staging.tiktok_shop_order_detail (
    etl_created_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_at DATETIME2 DEFAULT GETUTCDATE(),
    etl_batch_id NVARCHAR(50),
    etl_source NVARCHAR(50) DEFAULT 'tiktok_shop_api',
    order_id NVARCHAR(100) NOT NULL,
    order_status NVARCHAR(100),
    buyer_email NVARCHAR(500),
    buyer_message NVARCHAR(MAX),
    create_time BIGINT,
    update_time BIGINT,
    fulfillment_type NVARCHAR(100),
    payment_method_name NVARCHAR(200),
    warehouse_id NVARCHAR(100),
    user_id NVARCHAR(100),
    request_id NVARCHAR(200),
    shop_id NVARCHAR(100),
    region NVARCHAR(50),
    cancel_order_sla_time BIGINT,
    collection_due_time BIGINT,
    commerce_platform NVARCHAR(100),
    delivery_option_id NVARCHAR(100),
    delivery_option_name NVARCHAR(200),
    delivery_type NVARCHAR(100),
    fulfillment_priority_level INT,
    has_updated_recipient_address BIT,
    is_cod BIT,
    is_on_hold_order BIT,
    is_replacement_order BIT,
    is_sample_order BIT,
    order_type NVARCHAR(100),
    paid_time BIGINT,
    recommended_shipping_time BIGINT,
    rts_sla_time BIGINT,
    shipping_due_time BIGINT,
    shipping_provider NVARCHAR(200),
    shipping_provider_id NVARCHAR(100),
    shipping_type NVARCHAR(100),
    tts_sla_time BIGINT,
    tracking_number NVARCHAR(200),
    is_buyer_request_cancel BIT,
    cancel_reason NVARCHAR(MAX),
    cancel_user NVARCHAR(100),
    buyer_uid NVARCHAR(100),
    split_or_combine_tag NVARCHAR(100),
    seller_note NVARCHAR(MAX),
    delivery_option NVARCHAR(200),
    delivery_due_time BIGINT,
    rts_time BIGINT,
    delivery_sla_time BIGINT,
    collection_sla_time BIGINT,
    order_line_id NVARCHAR(100),
    cpf NVARCHAR(100),
    payment_currency NVARCHAR(10),
    payment_original_shipping_fee DECIMAL(18,4),
    payment_original_total_product_price DECIMAL(18,4),
    payment_platform_discount DECIMAL(18,4),
    payment_seller_discount DECIMAL(18,4),
    payment_shipping_fee DECIMAL(18,4),
    payment_shipping_fee_cofunded_discount DECIMAL(18,4),
    payment_shipping_fee_platform_discount DECIMAL(18,4),
    payment_shipping_fee_seller_discount DECIMAL(18,4),
    payment_sub_total DECIMAL(18,4),
    payment_tax DECIMAL(18,4),
    payment_total_amount DECIMAL(18,4),
    recipient_address_detail NVARCHAR(MAX),
    recipient_address_line1 NVARCHAR(MAX),
    recipient_address_line2 NVARCHAR(MAX),
    recipient_address_line3 NVARCHAR(MAX),
    recipient_address_line4 NVARCHAR(MAX),
    recipient_first_name NVARCHAR(MAX),
    recipient_first_name_local_script NVARCHAR(MAX),
    recipient_last_name NVARCHAR(MAX),
    recipient_last_name_local_script NVARCHAR(MAX),
    recipient_name NVARCHAR(MAX),
    recipient_full_address NVARCHAR(MAX),
    recipient_phone_number NVARCHAR(100),
    recipient_postal_code NVARCHAR(50),
    recipient_region_code NVARCHAR(50),
    recipient_district_info NVARCHAR(MAX),
    item_id NVARCHAR(100),
    item_product_id NVARCHAR(100),
    item_product_name NVARCHAR(1000),
    item_sku_id NVARCHAR(100),
    item_sku_name NVARCHAR(1000),
    item_sku_type NVARCHAR(100),
    item_sku_image NVARCHAR(MAX),
    item_seller_sku NVARCHAR(200),
    item_quantity INT,
    item_currency NVARCHAR(10),
    item_display_status NVARCHAR(100),
    item_is_gift BIT,
    item_original_price DECIMAL(18,4),
    item_sale_price DECIMAL(18,4),
    item_platform_discount DECIMAL(18,4),
    item_seller_discount DECIMAL(18,4),
    item_package_id NVARCHAR(100),
    item_package_status NVARCHAR(100),
    item_shipping_provider_id NVARCHAR(100),
    item_shipping_provider_name NVARCHAR(200),
    item_tracking_number NVARCHAR(200),
    item_cancel_reason NVARCHAR(MAX),
    item_cancel_user NVARCHAR(100),
    item_is_buyer_request_cancel BIT,
    item_rts_time BIGINT,
    item_shipped_time BIGINT,
    item_delivered_time BIGINT,
    item_sku_attributes NVARCHAR(MAX),
    package_id NVARCHAR(100),
    package_status NVARCHAR(100),
    package_shipping_provider_id NVARCHAR(100),
    package_shipping_provider_name NVARCHAR(200),
    package_tracking_number NVARCHAR(200),
    shop_cipher NVARCHAR(200),
    payment_info_json NVARCHAR(MAX),
    recipient_address_json NVARCHAR(MAX),
    line_items_json NVARCHAR(MAX),
    packages_json NVARCHAR(MAX),
    raw_order_json NVARCHAR(MAX),
    CONSTRAINT PK_tiktok_shop_order_detail PRIMARY KEY (order_id, item_id)
);
GO
CREATE INDEX IX_tiktok_shop_order_detail_order_id ON staging.tiktok_shop_order_detail (order_id);
CREATE INDEX IX_tiktok_shop_order_detail_item_id ON staging.tiktok_shop_order_detail (item_id);
CREATE INDEX IX_tiktok_shop_order_detail_create_time ON staging.tiktok_shop_order_detail (create_time);
CREATE INDEX IX_tiktok_shop_order_detail_update_time ON staging.tiktok_shop_order_detail (update_time);
CREATE INDEX IX_tiktok_shop_order_detail_etl_updated ON staging.tiktok_shop_order_detail (etl_updated_at);
GO

-- === STEP 6: CREATE SHOPEE ORDERS TABLES (prefixed with shopee_) ===
PRINT 'Step 6: Creating Shopee orders tables...';
GO

-- Drop children first, then parents (to avoid FK constraint errors)
IF OBJECT_ID('staging.shopee_buyer_proof_of_collection', 'U') IS NOT NULL DROP TABLE staging.shopee_buyer_proof_of_collection;
IF OBJECT_ID('staging.shopee_prescription_images', 'U') IS NOT NULL DROP TABLE staging.shopee_prescription_images;
IF OBJECT_ID('staging.shopee_order_warnings', 'U') IS NOT NULL DROP TABLE staging.shopee_order_warnings;
IF OBJECT_ID('staging.shopee_order_pending_terms', 'U') IS NOT NULL DROP TABLE staging.shopee_order_pending_terms;
IF OBJECT_ID('staging.shopee_payment_info', 'U') IS NOT NULL DROP TABLE staging.shopee_payment_info;
IF OBJECT_ID('staging.shopee_invoice', 'U') IS NOT NULL DROP TABLE staging.shopee_invoice;
IF OBJECT_ID('staging.shopee_package_items', 'U') IS NOT NULL DROP TABLE staging.shopee_package_items;
IF OBJECT_ID('staging.shopee_order_item_locations', 'U') IS NOT NULL DROP TABLE staging.shopee_order_item_locations;
IF OBJECT_ID('staging.shopee_order_items', 'U') IS NOT NULL DROP TABLE staging.shopee_order_items;
IF OBJECT_ID('staging.shopee_packages', 'U') IS NOT NULL DROP TABLE staging.shopee_packages;
IF OBJECT_ID('staging.shopee_recipient_address', 'U') IS NOT NULL DROP TABLE staging.shopee_recipient_address;
IF OBJECT_ID('staging.shopee_orders', 'U') IS NOT NULL DROP TABLE staging.shopee_orders;
GO

-- Create orders table
CREATE TABLE staging.shopee_orders (
    order_sn NVARCHAR(50) PRIMARY KEY,
    region NVARCHAR(10),
    currency NVARCHAR(10),
    cod BIT,
    total_amount DECIMAL(18,4),
    order_status NVARCHAR(50),
    shipping_carrier NVARCHAR(100),
    payment_method NVARCHAR(100),
    estimated_shipping_fee DECIMAL(18,4),
    message_to_seller NVARCHAR(MAX),
    create_time DATETIME2,
    update_time DATETIME2,
    days_to_ship INT,
    ship_by_date DATETIME2,
    buyer_user_id BIGINT,
    buyer_username NVARCHAR(100),
    actual_shipping_fee DECIMAL(18,4),
    actual_shipping_fee_confirmed BIT,
    goods_to_declare BIT,
    note NVARCHAR(MAX),
    note_update_time DATETIME2,
    pay_time DATETIME2,
    dropshipper NVARCHAR(100),
    dropshipper_phone NVARCHAR(50),
    split_up BIT,
    buyer_cancel_reason NVARCHAR(MAX),
    cancel_by NVARCHAR(100),
    cancel_reason NVARCHAR(MAX),
    buyer_cpf_id NVARCHAR(50),
    fulfillment_flag NVARCHAR(50),
    pickup_done_time DATETIME2,
    reverse_shipping_fee DECIMAL(18,4),
    order_chargeable_weight_gram INT,
    prescription_check_status INT,
    pharmacist_name NVARCHAR(100),
    prescription_approval_time DATETIME2,
    prescription_rejection_time DATETIME2,
    edt_from DATETIME2,
    edt_to DATETIME2,
    booking_sn NVARCHAR(50),
    advance_package BIT,
    return_request_due_date DATETIME2,
    is_buyer_shop_collection BIT,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- Create recipient_address table
CREATE TABLE staging.shopee_recipient_address (
    order_sn NVARCHAR(50) PRIMARY KEY,
    name NVARCHAR(200),
    phone NVARCHAR(50),
    town NVARCHAR(100),
    district NVARCHAR(100),
    city NVARCHAR(100),
    state NVARCHAR(100),
    region NVARCHAR(100),
    zipcode NVARCHAR(20),
    full_address NVARCHAR(MAX),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create order_items table
CREATE TABLE staging.shopee_order_items (
    order_sn NVARCHAR(50) NOT NULL,
    order_item_id BIGINT NOT NULL,
    item_id BIGINT,
    item_name NVARCHAR(500),
    item_sku NVARCHAR(100),
    model_id BIGINT,
    model_name NVARCHAR(500),
    model_sku NVARCHAR(100),
    model_quantity_purchased INT,
    model_original_price DECIMAL(18,4),
    model_discounted_price DECIMAL(18,4),
    wholesale BIT,
    weight DECIMAL(18,4),
    add_on_deal BIT,
    main_item BIT,
    add_on_deal_id BIGINT,
    promotion_type NVARCHAR(50),
    promotion_id BIGINT,
    promotion_group_id BIGINT,
    is_prescription_item BIT,
    is_b2c_owned_item BIT,
    consultation_id NVARCHAR(100),
    image_url NVARCHAR(MAX),
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, order_item_id),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create order_item_locations table
CREATE TABLE staging.shopee_order_item_locations (
    order_sn NVARCHAR(50) NOT NULL,
    order_item_id BIGINT NOT NULL,
    location_id NVARCHAR(100) NOT NULL,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, order_item_id, location_id),
    FOREIGN KEY (order_sn, order_item_id) REFERENCES staging.shopee_order_items(order_sn, order_item_id) ON DELETE CASCADE
);
GO

-- Create packages table
CREATE TABLE staging.shopee_packages (
    order_sn NVARCHAR(50) NOT NULL,
    package_number NVARCHAR(100) NOT NULL,
    logistics_status NVARCHAR(50),
    logistics_channel_id BIGINT,
    shipping_carrier NVARCHAR(100),
    allow_self_design_awb BIT,
    parcel_chargeable_weight_gram INT,
    group_shipment_id BIGINT,
    virtual_contact_number NVARCHAR(50),
    package_query_number NVARCHAR(100),
    sorting_group NVARCHAR(100),
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, package_number),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create package_items table
CREATE TABLE staging.shopee_package_items (
    order_sn NVARCHAR(50) NOT NULL,
    package_number NVARCHAR(100) NOT NULL,
    order_item_id BIGINT NOT NULL,
    item_id BIGINT,
    model_id BIGINT,
    model_quantity INT,
    promotion_group_id BIGINT,
    product_location_id NVARCHAR(100),
    parcel_chargeable_weight INT,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, package_number, order_item_id),
    FOREIGN KEY (order_sn, package_number) REFERENCES staging.shopee_packages(order_sn, package_number) ON DELETE NO ACTION,
    FOREIGN KEY (order_sn, order_item_id) REFERENCES staging.shopee_order_items(order_sn, order_item_id) ON DELETE NO ACTION
);
GO

-- Create invoice table
CREATE TABLE staging.shopee_invoice (
    order_sn NVARCHAR(50) PRIMARY KEY,
    number NVARCHAR(100),
    series_number NVARCHAR(100),
    access_key NVARCHAR(200),
    issue_date DATETIME2,
    total_value DECIMAL(18,4),
    products_total_value DECIMAL(18,4),
    tax_code NVARCHAR(50),
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create payment_info table
CREATE TABLE staging.shopee_payment_info (
    order_sn NVARCHAR(50) NOT NULL,
    transaction_id NVARCHAR(100) NOT NULL,
    payment_method NVARCHAR(100),
    payment_processor_register NVARCHAR(100),
    card_brand NVARCHAR(50),
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, transaction_id),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create order_pending_terms table
CREATE TABLE staging.shopee_order_pending_terms (
    order_sn NVARCHAR(50) NOT NULL,
    term NVARCHAR(400) NOT NULL,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, term),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create order_warnings table
CREATE TABLE staging.shopee_order_warnings (
    order_sn NVARCHAR(50) NOT NULL,
    warning NVARCHAR(400) NOT NULL,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, warning),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create prescription_images table
CREATE TABLE staging.shopee_prescription_images (
    order_sn NVARCHAR(50) NOT NULL,
    image_url NVARCHAR(400) NOT NULL,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, image_url),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create buyer_proof_of_collection table
CREATE TABLE staging.shopee_buyer_proof_of_collection (
    order_sn NVARCHAR(50) NOT NULL,
    image_url NVARCHAR(400) NOT NULL,
    source_request_id NVARCHAR(100),
    ingested_at DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (order_sn, image_url),
    FOREIGN KEY (order_sn) REFERENCES staging.shopee_orders(order_sn) ON DELETE CASCADE
);
GO

-- Create indexes for Shopee tables
CREATE INDEX IX_shopee_orders_update_time ON staging.shopee_orders(update_time);
CREATE INDEX IX_shopee_orders_order_status ON staging.shopee_orders(order_status);
CREATE INDEX IX_shopee_orders_create_time ON staging.shopee_orders(create_time);
CREATE INDEX IX_shopee_orders_buyer_user_id ON staging.shopee_orders(buyer_user_id);

CREATE INDEX IX_shopee_order_items_item_id ON staging.shopee_order_items(item_id);
CREATE INDEX IX_shopee_order_items_model_id ON staging.shopee_order_items(model_id);

CREATE INDEX IX_shopee_packages_logistics_status ON staging.shopee_packages(logistics_status);
CREATE INDEX IX_shopee_packages_shipping_carrier ON staging.shopee_packages(shipping_carrier);

CREATE INDEX IX_shopee_payment_info_transaction_id ON staging.shopee_payment_info(transaction_id);
GO

PRINT 'Shopee orders tables created successfully!';
GO

-- === STEP 7: CREATE AUXILIARY TABLES ===
PRINT 'Step 7: Creating auxiliary tables (token storage, etc.)...';
GO

IF OBJECT_ID('etl_control.api_token_storage', 'U') IS NOT NULL DROP TABLE etl_control.api_token_storage;
GO
CREATE TABLE etl_control.api_token_storage (
    platform NVARCHAR(50) PRIMARY KEY,
    access_token NVARCHAR(MAX) NOT NULL,
    refresh_token NVARCHAR(MAX),
    expires_at DATETIME2,
    shop_cipher NVARCHAR(255),
    last_updated DATETIME2 DEFAULT GETUTCDATE()
);
GO

PRINT '--- Master Database Setup Finished ---';
GO



