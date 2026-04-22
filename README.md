# Competitor Pricing Intelligence System

## Overview

Businesses often need to compare tools like Notion, Slack, Trello, and Airtable based on pricing and features.
However, this data is scattered across websites and not structured for analysis.

This project builds a data pipeline that collects, processes, and presents competitor pricing data in a usable format.

---

## Quick Start (Run the System)

```bash
pip install -r requirements.txt

python run_pipeline.py

streamlit run dashboard/app.py
```

* Step 1: Runs the full pipeline (scraping → processing → storage)
* Step 2: Launches the dashboard

---

## What this system does

* Scrapes pricing plans and feature data
* Cleans and standardizes the data
* Processes features into structured signals
* Computes value-based metrics
* Stores results in a database
* Provides a dashboard for decision-making

---

## System Workflow

```text
Scraping → Cleaning → Feature Processing → Scoring → Storage → Dashboard
```

---

## Components

### Scraping

* Collects data from Notion, Slack, Trello, Airtable
* Includes fallback handling for reliability

---

### Data Cleaning

* Standardizes pricing
* Removes noise and inconsistencies

---

### Feature Processing

* Extracts meaningful feature tags
* Deduplicates and filters signals

---

### Scoring Logic

Evaluates plans based on:

* Feature coverage
* Price
* Relevance

Used for ranking and recommendations.

---

### Storage

* SQLite database
* Lightweight and easy to run locally

---

### Dashboard

Provides:

* Pricing comparison
* Budget-based selection
* Plan comparison
* Recommendations
* Price vs value visualization

---

## Automation

The pipeline runs end-to-end using:

```bash
python run_pipeline.py
```

It can be scheduled using cron or task scheduler if needed.

---

## Design Considerations

* Pricing pages are dynamic and may change
* Scraping may occasionally miss some data
* A fallback dataset ensures system continuity

---

## Summary

This project demonstrates how to build a practical data pipeline that converts unstructured web data into structured insights.
