import pandas as pd
from datetime import datetime
import os

olist_Dataset_location = "/Volumes/dev_catalog/bronze_operational/olist_landing"
batch_01_output_path = "/Volumes/dev_catalog/bronze_operational/olist_landing/batch_01_initial"
batch_02_output_path = "/Volumes/dev_catalog/bronze_operational/olist_landing/batch_02_incremental"

# Create output directories if they don't exist
os.makedirs(batch_01_output_path, exist_ok=True)
os.makedirs(batch_02_output_path, exist_ok=True)

# ---------------------------------------------------------
# STEP 1 — Load orders, split chronologically
# ---------------------------------------------------------
df = pd.read_csv(f"{olist_Dataset_location}/olist_orders_dataset.csv")
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

batch_01 = df[df['order_purchase_timestamp'] < '2018-07-01'].copy()
batch_02 = df[df['order_purchase_timestamp'] >= '2018-07-01'].copy()

# STEP2: Filter order_items and order_payments to match
# order_id membership in each batch. Injected rows reuse
# real order_ids, so no parallel injection needed here
# filtering on the ORIGINAL order_id set is sufficient.
# ---------------------------------------------------------
batch_01_order_ids = set(batch_01['order_id'])
batch_02_order_ids = set(batch_02['order_id'])  # pre-injection, real IDs only

order_items = pd.read_csv(f"{olist_Dataset_location}/olist_order_items_dataset.csv")
order_payments = pd.read_csv(f"{olist_Dataset_location}/olist_order_payments_dataset.csv")

order_items_b01 = order_items[order_items['order_id'].isin(batch_01_order_ids)]
order_items_b02 = order_items[order_items['order_id'].isin(batch_02_order_ids)]

order_payments_b01 = order_payments[order_payments['order_id'].isin(batch_01_order_ids)]
order_payments_b02 = order_payments[order_payments['order_id'].isin(batch_02_order_ids)]

# ---------------------------------------------------------
# STEP 3: Write everything out
# ---------------------------------------------------------
batch_01.to_csv(f"{batch_01_output_path}/olist_orders_dataset.csv", index=False)
batch_02.to_csv(f"{batch_02_output_path}/olist_orders_dataset.csv", index=False)

order_items_b01.to_csv(f"{batch_01_output_path}/olist_order_items_dataset.csv", index=False)
order_items_b02.to_csv(f"{batch_02_output_path}/olist_order_items_dataset", index=False)

order_payments_b01.to_csv(f"{batch_01_output_path}/olist_order_payments_dataset", index=False)
order_payments_b02.to_csv(f"{batch_02_output_path}/olist_order_payments_dataset", index=False)

