# FakeStore to Google BigQuery Pipeline

An Extract, Transform, Load (ETL) pipeline written in Python that ingests product data from the public FakeStoreAPI, cleans and flattens the nested data using Pandas, and loads the result directly into a staging table in Google BigQuery.

This project is a classic example of an Analytics Engineering data ingestion task.

---

## Project Overview

The pipeline performs the following steps:

1.  **Extract (E):** Fetches the full product catalog via a `GET` request to `https://fakestoreapi.com/products`.
2.  **Transform (T):** Uses Pandas to load the JSON data, flatten the nested `rating` object (separating `rate` and `count`), cast columns to appropriate types (`Int64`, `float`), and add an audit timestamp.
3.  **Load (L):** Uses the `google-cloud-bigquery` client to load the transformed Pandas DataFrame into a specified BigQuery table using the `WRITE_TRUNCATE` disposition (full refresh).
