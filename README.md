# Marketplace Intelligent Plarform
A reliable data intelligent platform for a Marketplace company that supports data driven decision making.

---

# Overview
Marketplace businesses depend on healthy interactions between customers, sellers, orders, payments, and delivery operations. Poor delivery performance or declining seller activity can affect customer experience, marketplace demand, and seller retention.

This project simulates the data platform of an e-commerce marketplace. It transforms operational datasets into validated analytical models that support marketplace performance measurement and seller-level operational analysis.

The primary engineering objective is to build a reproducible data pipeline with explicit data-quality rules, well-defined analytical models, and trustworthy business metrics.

## Example scenario:
```
Customer churns
    -> fewer orders placed
        -> seller GMV on Olist drops
            -> seller sees poor ROI on their Olist subscription
                -> seller churns
                    -> fewer products on platform
                        -> remaining customers have less selection
                            -> more customers churn

NOTE: 
This is a demand-supply death spiral.
It's the existential risk for any two-sided marketplace.
One side leaving accelerates the other side leaving.
```
A **data platform gives a Marketplace enough lead time to intervene before the spiral starts**. By the time churn is visible in revenue, you're already 2-3 months into the problem. An analytics platform surfaces it earlier.

---

# Business questions:
**The Platform is designed to answer:**
- How do order volume and marketplace transaction value change over time? 
- What is the average order value under a clearly defined business rule?
- Which sellers show declining activity or potential inactivity?
- Which sellers appear to contribute to poor delivery performance?
- What percentage of orders arrive on time? 
- How long does it take for sellers to hand orders to carriers?
- How long do orders spend in transit?

---

# Architecture
![Project Screenshot](Marketplace%20Dataplatform%20Image.png)

---
# Future enhancements:
### Ingestion reliability:
- Incremental ingestion
- Retry/idempotency handling
- Data quality expectations
- Quarantine and reconciliation

### Schema reliability:
- Schema evolution
- Schema breakage detection
- Contract validation

### Analytical depth:
- Customer cohort retention
- Seller cohort analysis
- Root-cause analysis
Current scope and limitations:

# Current version:
- Static source data
- Full-load pipeline
- No CDC
- No SCD Type 2
- No production source-system integration
- Synthetic incremental/retry scenarios not yet implemented
- Descriptive rather than causal analytics
