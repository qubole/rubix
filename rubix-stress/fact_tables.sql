-- Fact tables' DDLs
-- Internally used fact tables are such that total size of fact data is enough
-- to cause invalidations due to disk space.

-- Fact tables used in Qubole tests
--  store_sales table backed by single 15GB file
--  store_sales table backed by 15 files, 15GB each
--  store_sales table backed by 64k files of ~300kb each
--  Mix of different scales, formats of standard store_sales

create database if not exists rubix_stress;
use rubix_stress;

-- Add Fact tables' DDL below, remove the sample one

create external table if not exists store_sales_orc_scale100
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float
)
stored as orc
location '<LOCATION>';
