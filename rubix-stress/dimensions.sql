create database if not exists rubix_stress;
use rubix_stress;

-- Replace dimension tables DDLs

CREATE EXTERNAL TABLE IF NOT EXISTS customer (c_customer_sk bigint , c_customer_id string ,
c_current_cdemo_sk bigint , c_current_hdemo_sk bigint , c_current_addr_sk bigint , c_first_shipto_date_sk bigint , c_first_sales_date_sk bigint , c_salutation string ,
c_first_name string ,
c_last_name string ,
c_preferred_cust_flag string ,
c_birth_day int , c_birth_month int , c_birth_year int , c_birth_country string ,
c_login string ,
c_email_address string ,
c_last_review_date string)
STORED AS orc LOCATION '<LOCATION>';


CREATE EXTERNAL TABLE IF NOT EXISTS customer_address ( ca_address_sk bigint , ca_address_id string ,
ca_street_number string ,
ca_street_name string ,
ca_street_type string ,
ca_suite_number string ,
ca_city string ,
ca_county string ,
ca_state string ,
ca_zip string ,
ca_country string ,
ca_gmt_offset decimal(5,2) ,
ca_location_type string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS customer_demographics ( cd_demo_sk bigint, cd_gender string ,
cd_marital_status string ,
cd_education_status string ,
cd_purchase_estimate int , cd_credit_rating string ,
cd_dep_count int , cd_dep_employed_count int , cd_dep_college_count int)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS date_dim ( d_date_sk bigint , d_date_id string ,
d_date string ,
d_month_seq int , d_week_seq int , d_quarter_seq int , d_year int , d_dow int , d_moy int , d_dom int , d_qoy int , d_fy_year int , d_fy_quarter_seq int , d_fy_week_seq int , d_day_name string ,
d_quarter_name string ,
d_holiday string ,
d_weekend string ,
d_following_holiday string ,
d_first_dom int , d_last_dom int , d_same_day_ly int , d_same_day_lq int , d_current_day string ,
d_current_week string ,
d_current_month string ,
d_current_quarter string ,
d_current_year string)
STORED AS orc LOCATION '<LOCATION>';


CREATE EXTERNAL TABLE IF NOT EXISTS household_demographics ( hd_demo_sk bigint , hd_income_band_sk bigint , hd_buy_potential string ,
hd_dep_count int , hd_vehicle_count int)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS income_band ( ib_income_band_sk bigint , ib_lower_bound int , ib_upper_bound int)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS item ( i_item_sk bigint , i_item_id string ,
i_rec_start_date string ,
i_rec_end_date string ,
i_item_desc string ,
i_current_price decimal(7,2) ,
i_wholesale_cost decimal(7,2) ,
i_brand_id int , i_brand string ,
i_class_id int , i_class string ,
i_category_id int , i_category string ,
i_manufact_id int , i_manufact string ,
i_size string ,
i_formulation string ,
i_color string ,
i_units string ,
i_container string ,
i_manager_id int , i_product_name string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS promotion ( p_promo_sk bigint , p_promo_id string ,
p_start_date_sk bigint , p_end_date_sk bigint , p_item_sk bigint , p_cost decimal(15,2) ,
p_response_target int , p_promo_name string ,
p_channel_dmail string ,
p_channel_email string ,
p_channel_catalog string ,
p_channel_tv string ,
p_channel_radio string ,
p_channel_press string ,
p_channel_event string ,
p_channel_demo string ,
p_channel_details string ,
p_purpose string ,
p_discount_active string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS reason ( r_reason_sk bigint , r_reason_id string ,
r_reason_desc string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS ship_mode ( sm_ship_mode_sk bigint , sm_ship_mode_id string ,
sm_type string ,
sm_code string ,
sm_carrier string ,
sm_contract string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS store ( s_store_sk bigint , s_store_id string ,
s_rec_start_date string ,
s_rec_end_date string ,
s_closed_date_sk bigint , s_store_name string ,
s_number_employees int , s_floor_space int , s_hours string ,
s_manager string ,
s_market_id int , s_geography_class string ,
s_market_desc string ,
s_market_manager string ,
s_division_id int , s_division_name string ,
s_company_id int , s_company_name string ,
s_street_number string ,
s_street_name string ,
s_street_type string ,
s_suite_number string ,
s_city string ,
s_county string ,
s_state string ,
s_zip string ,
s_country string ,
s_gmt_offset decimal(5,2) ,
s_tax_precentage decimal(5,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS time_dim ( t_time_sk bigint , t_time_id string ,
t_time int , t_hour int , t_minute int , t_second int , t_am_pm string ,
t_shift string ,
t_sub_shift string ,
t_meal_time string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse ( w_warehouse_sk bigint , w_warehouse_id string ,
w_warehouse_name string ,
w_warehouse_sq_ft int , w_street_number string ,
w_street_name string ,
w_street_type string ,
w_suite_number string ,
w_city string ,
w_county string ,
w_state string ,
w_zip string ,
w_country string ,
w_gmt_offset decimal(5,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS web_site ( web_site_sk bigint , web_site_id string ,
web_rec_start_date string ,
web_rec_end_date string ,
web_name string ,
web_open_date_sk bigint , web_close_date_sk bigint , web_class string ,
web_manager string ,
web_mkt_id int , web_mkt_class string ,
web_mkt_desc string ,
web_market_manager string ,
web_company_id int , web_company_name string ,
web_street_number string ,
web_street_name string ,
web_street_type string ,
web_suite_number string ,
web_city string ,
web_county string ,
web_state string ,
web_zip string ,
web_country string ,
web_gmt_offset decimal(5,2) ,
web_tax_percentage decimal(5,2))
STORED AS orc LOCATION '<LOCATION>';


CREATE EXTERNAL TABLE IF NOT EXISTS web_page ( wp_web_page_sk bigint , wp_web_page_id string ,
wp_rec_start_date string ,
wp_rec_end_date string ,
wp_creation_date_sk bigint , wp_access_date_sk bigint , wp_autogen_flag string ,
wp_customer_sk bigint , wp_url string ,
wp_type string ,
wp_char_count int , wp_link_count int , wp_image_count int , wp_max_ad_count int)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS inventory ( inv_date_sk bigint , inv_item_sk bigint , inv_warehouse_sk bigint , inv_quantity_on_hand int)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS store_returns ( sr_returned_date_sk bigint , sr_return_time_sk bigint , sr_item_sk bigint , sr_customer_sk bigint , sr_cdemo_sk bigint , sr_hdemo_sk bigint , sr_addr_sk bigint , sr_store_sk bigint , sr_reason_sk bigint , sr_ticket_number bigint , sr_return_quantity int , sr_return_amt decimal(7,2) ,
sr_return_tax decimal(7,2) ,
sr_return_amt_inc_tax decimal(7,2) ,
sr_fee decimal(7,2) ,
sr_return_ship_cost decimal(7,2) ,
sr_refunded_cash decimal(7,2) ,
sr_reversed_charge decimal(7,2) ,
sr_store_credit decimal(7,2) ,
sr_net_loss decimal(7,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS web_sales ( ws_sold_date_sk bigint , ws_sold_time_sk bigint , ws_ship_date_sk bigint , ws_item_sk bigint , ws_bill_customer_sk bigint , ws_bill_cdemo_sk bigint , ws_bill_hdemo_sk bigint , ws_bill_addr_sk bigint , ws_ship_customer_sk bigint , ws_ship_cdemo_sk bigint , ws_ship_hdemo_sk bigint , ws_ship_addr_sk bigint , ws_web_page_sk bigint , ws_web_site_sk bigint , ws_ship_mode_sk bigint , ws_warehouse_sk bigint , ws_promo_sk bigint , ws_order_number bigint , ws_quantity int , ws_wholesale_cost decimal(7,2) ,
ws_list_price decimal(7,2) ,
ws_sales_price decimal(7,2) ,
ws_ext_discount_amt decimal(7,2) ,
ws_ext_sales_price decimal(7,2) ,
ws_ext_wholesale_cost decimal(7,2) ,
ws_ext_list_price decimal(7,2) ,
ws_ext_tax decimal(7,2) ,
ws_coupon_amt decimal(7,2) ,
ws_ext_ship_cost decimal(7,2) ,
ws_net_paid decimal(7,2) ,
ws_net_paid_inc_tax decimal(7,2) ,
ws_net_paid_inc_ship decimal(7,2) ,
ws_net_paid_inc_ship_tax decimal(7,2) ,
ws_net_profit decimal(7,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS web_returns ( wr_returned_date_sk bigint , wr_returned_time_sk bigint , wr_item_sk bigint , wr_refunded_customer_sk bigint , wr_refunded_cdemo_sk bigint , wr_refunded_hdemo_sk bigint , wr_refunded_addr_sk bigint , wr_returning_customer_sk bigint , wr_returning_cdemo_sk bigint , wr_returning_hdemo_sk bigint , wr_returning_addr_sk bigint , wr_web_page_sk bigint , wr_reason_sk bigint , wr_order_number bigint , wr_return_quantity int , wr_return_amt decimal(7,2) ,
wr_return_tax decimal(7,2) ,
wr_return_amt_inc_tax decimal(7,2) ,
wr_fee decimal(7,2) ,
wr_return_ship_cost decimal(7,2) ,
wr_refunded_cash decimal(7,2) ,
wr_reversed_charge decimal(7,2) ,
wr_account_credit decimal(7,2) ,
wr_net_loss decimal(7,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS call_center ( cc_call_center_sk bigint , cc_call_center_id string,
cc_rec_start_date string,
cc_rec_end_date string,
cc_closed_date_sk int , cc_open_date_sk int , cc_name string,
cc_class string,
cc_employees int , cc_sq_ft int , cc_hours string,
cc_manager string,
cc_mkt_id int , cc_mkt_class string,
cc_mkt_desc string,
cc_market_manager string,
cc_division int , cc_division_name string,
cc_company int , cc_company_name string,
cc_street_number string,
cc_street_name string,
cc_street_type string,
cc_suite_number string,
cc_city string,
cc_county string,
cc_state string,
cc_zip string,
cc_country string,
cc_gmt_offset decimal(5,2),
cc_tax_percentage decimal(5,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_page ( cp_catalog_page_sk bigint , cp_catalog_page_id string,
cp_start_date_sk int , cp_end_date_sk int , cp_department string,
cp_catalog_number int , cp_catalog_page_number int , cp_description string,
cp_type string)
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_returns ( cr_returned_date_sk bigint , cr_returned_time_sk bigint , cr_item_sk bigint , cr_refunded_customer_sk bigint , cr_refunded_cdemo_sk bigint , cr_refunded_hdemo_sk bigint , cr_refunded_addr_sk bigint , cr_returning_customer_sk bigint , cr_returning_cdemo_sk bigint , cr_returning_hdemo_sk bigint , cr_returning_addr_sk bigint , cr_call_center_sk bigint , cr_catalog_page_sk bigint , cr_ship_mode_sk bigint , cr_warehouse_sk bigint , cr_reason_sk bigint , cr_order_number bigint , cr_return_quantity int , cr_return_amount decimal(7,2),
cr_return_tax decimal(7,2),
cr_return_amt_inc_tax decimal(7,2),
cr_fee decimal(7,2),
cr_return_ship_cost decimal(7,2),
cr_refunded_cash decimal(7,2),
cr_reversed_charge decimal(7,2),
cr_store_credit decimal(7,2),
cr_net_loss decimal(7,2))
STORED AS orc LOCATION '<LOCATION>';

CREATE EXTERNAL TABLE IF NOT EXISTS catalog_sales ( cs_sold_date_sk bigint , cs_sold_time_sk bigint , cs_ship_date_sk bigint , cs_bill_customer_sk bigint , cs_bill_cdemo_sk bigint , cs_bill_hdemo_sk bigint , cs_bill_addr_sk bigint , cs_ship_customer_sk bigint , cs_ship_cdemo_sk bigint , cs_ship_hdemo_sk bigint , cs_ship_addr_sk bigint , cs_call_center_sk bigint , cs_catalog_page_sk bigint , cs_ship_mode_sk bigint , cs_warehouse_sk bigint , cs_item_sk bigint , cs_promo_sk bigint , cs_order_number bigint , cs_quantity int , cs_wholesale_cost decimal(7,2),
cs_list_price decimal(7,2),
cs_sales_price decimal(7,2),
cs_ext_discount_amt decimal(7,2),
cs_ext_sales_price decimal(7,2),
cs_ext_wholesale_cost decimal(7,2),
cs_ext_list_price decimal(7,2),
cs_ext_tax decimal(7,2),
cs_coupon_amt decimal(7,2),
cs_ext_ship_cost decimal(7,2),
cs_net_paid decimal(7,2),
cs_net_paid_inc_tax decimal(7,2),
cs_net_paid_inc_ship decimal(7,2),
cs_net_paid_inc_ship_tax decimal(7,2),
cs_net_profit decimal(7,2))
STORED AS orc LOCATION '<LOCATION>';



