import pandas as pd
from datetime import datetime

olist_Dataset_location = "/Volumes/marketplace/olist/datasets/"
output_path = "/Volumes/marketplace/olist/pipeline_batches/"

# ---------------------------------------------------------
# STEP 1 — Load orders, split chronologically
# ---------------------------------------------------------
df = pd.read_csv(f"{olist_Dataset_location}/olist_orders_dataset.csv")
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

batch_01 = df[df['order_purchase_timestamp'] < '2018-07-01'].copy()
batch_02 = df[df['order_purchase_timestamp'] >= '2018-07-01'].copy()

# ---------------------------------------------------------
# STEP 2 — Inject retry duplicates (Injection B)
# creating a new column "ingested_at" 
# datetime.now() is used to get the current date and time
# ---------------------------------------------------------
batch_02["ingested_at"] = datetime.now()

duplicate_records = batch_02.sample(120).copy()
duplicate_records['ingested_at'] = duplicate_records['ingested_at'] + pd.Timedelta(5, 'm')

# ---------------------------------------------------------
# STEP 3 — Inject null customer_id (Injection A)
# excluded based on order_id, not customer_id (customer_id gets mutated)
# ---------------------------------------------------------
null_records = batch_02[~batch_02["order_id"].isin(duplicate_records["order_id"])].sample(144).copy()
null_records["customer_id"] = pd.NA

temp_data = pd.concat([duplicate_records, null_records])

# ---------------------------------------------------------
# STEP 4 — Inject compound case: null on first attempt,
# corrected + duplicated on retry (Version B)
# ---------------------------------------------------------
duplicate_null_records = batch_02[~batch_02['order_id'].isin(temp_data['order_id'])].sample(4).copy()

duplicate_null_records_before_retry = duplicate_null_records.copy()
duplicate_null_records_before_retry["customer_id"] = pd.NA

duplicate_null_records_after_retry = duplicate_null_records.copy()
duplicate_null_records_after_retry['ingested_at'] = duplicate_null_records_after_retry['ingested_at'] + pd.Timedelta(5, 'm')

duplicate_null_records_final = pd.concat([duplicate_null_records_before_retry, duplicate_null_records_after_retry])

# ---------------------------------------------------------
# STEP 5 — Verify non-overlap (proof, not assumption)
# Why set()? 
#   set is undordered collection of unique elements/values, with duplicates automatically reomoved.
#   The & operator between two sets performs intersection; returns only the value present in both sides.   
# ---------------------------------------------------------
overlap_1 = set(duplicate_null_records['order_id']) & set(duplicate_records['order_id'])
overlap_2 = set(duplicate_null_records['order_id']) & set(null_records['order_id'])
print(f"Overlap with duplicate_records: {overlap_1}")
print(f"Overlap with null_records: {overlap_2}")

# ---------------------------------------------------------
# STEP 6: Assemble final incremental batch
# ---------------------------------------------------------
Incremental_batch_02 = pd.concat([batch_02, temp_data, duplicate_null_records_final])

# ---------------------------------------------------------
# STEP 7: Filter order_items and order_payments to match
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
# STEP 8: Write everything out
# ---------------------------------------------------------
batch_01.to_csv(f"{output_path}/batch_01_initial/orders.csv", index=False)
Incremental_batch_02.to_csv(f"{output_path}/batch_02_incremental/orders.csv", index=False)

order_items_b01.to_csv(f"{output_path}/batch_01_initial/order_items.csv", index=False)
order_items_b02.to_csv(f"{output_path}/batch_02_incremental/order_items.csv", index=False)

order_payments_b01.to_csv(f"{output_path}/batch_01_initial/order_payments.csv", index=False)
order_payments_b02.to_csv(f"{output_path}/batch_02_incremental/order_payments.csv", index=False)

# ---------------------------------------------------------
# STEP 9: Summary log
# ---------------------------------------------------------
print(f"batch_01 orders: {len(batch_01)}, items: {len(order_items_b01)}, payments: {len(order_payments_b01)}")
print(f"batch_02 orders (clean): {len(batch_02)}")
print(f"  + duplicates injected: {len(duplicate_records)}")
print(f"  + nulls injected: {len(null_records)}")
print(f"  + null-then-retry pairs: {len(duplicate_null_records_final)}")
print(f"  = total incremental rows: {len(Incremental_batch_02)}")
print(f"batch_02 items: {len(order_items_b02)}, payments: {len(order_payments_b02)}")