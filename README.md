# Crypto Coin Price Alert System (GCP + Dataform + Airflow)

A real-time data engineering project that alerts users via email when their selected cryptocurrency (Bitcoin, Ethereum, Solana) crosses a price threshold.

---

## Architecture Overview

![image](https://github.com/user-attachments/assets/07907b6a-33b1-48b2-bfce-6083d8e1decf)


---

## Tech Stack

- **Cloud Functions**: Two separate functions for data ingestion and alerting
- **Pub/Sub**: Used for ingesting real-time API data
- **BigQuery**: Staging, transformation, and alert tracking
- **Dataform**: Data modeling and transformation
- **SendGrid**: For sending alert emails
- **Cloud Composer (Airflow)**: DAG to orchestrate the end-to-end pipeline

---

## Flow Summary

1. **Ingestion Function (`fetch_and_publish_price.py`)**  
   Fetches crypto prices from CoinGecko API for predefined coins, and publishes messages to a Pub/Sub topic.

2. **Pub/Sub**  
   Streams messages into a BigQuery staging table (`crypto_coin_stage`).

3. **Dataform Transformations**  
   - Joins user threshold preferences from `users_thresholds`
   - Compares incoming prices to thresholds
   - Stores violations in `crypto_threshold_violations`

4. **Alert Function (`send_price_alert.py`)**  
   - Reads from `crypto_threshold_violations`
   - Checks alert history to prevent duplicate alerts
   - Sends alert email via SendGrid
   - Logs each alert in `alert_history`

5. **Airflow DAG (`crypto_alert_dag`)**  
   Automates the flow:
   - Triggers data ingestion function
   - Compiles and executes Dataform workflow
   - Triggers alert function


