CREATE CATALOG IF NOT EXISTS dev_catalog;

CREATE SCHEMA IF NOT EXISTS dev_catalog.bronze_operational;

CREATE SCHEMA IF NOT EXISTS dev_catalog.silver_clean;

CREATE SCHEMA IF NOT EXISTS dev_catalog.silver_quarantine;

CREATE SCHEMA IF NOT EXISTS dev_catalog.gold_warehouse;

CREATE VOLUME IF NOT EXISTS dev_catalog.bronze_operational.olist_landing;