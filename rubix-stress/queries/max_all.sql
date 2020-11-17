-- max_all

use rubix_stress;
select
max(ss_sold_date_sk),
max(ss_sold_time_sk),
max(ss_item_sk),
max(ss_customer_sk),
max(ss_cdemo_sk),
max(ss_hdemo_sk),
max(ss_addr_sk),
max(ss_store_sk),
max(ss_promo_sk),
max(ss_ticket_number),
max(ss_quantity),
max(ss_wholesale_cost),
max(ss_list_price),
max(ss_sales_price),
max(ss_ext_discount_amt),
max(ss_ext_sales_price),
max(ss_ext_wholesale_cost),
max(ss_ext_list_price),
max(ss_ext_tax),
max(ss_coupon_amt),
max(ss_net_paid),
max(ss_net_paid_inc_tax),
max(ss_net_profit)
from %FACT_TABLE%;