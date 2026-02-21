# ReviewSense AI

## Overview

ReviewSense AI is a Customer Review Insights Copilot built on Snowflake and GenAI. It analyzes Amazon product reviews to generate insights such as sentiment, themes, complaints, and improvement suggestions.

## Problem

Customer reviews are unstructured and difficult to analyze manually at scale.

## Solution

We built a Snowflake-based data pipeline and GenAI workflow to:

- Ingest and clean review data
- Structure reviews for analytics
- Enable embedding and RAG workflows
- Generate product insights using Cortex LLM

## Architecture

Pipeline layers:

RAW → CURATED → ANALYTICS → GenAI

```mermaid
flowchart TB
    AIRFLOW["Apache Airflow (Orchestrator)"]

    subgraph BRONZE["BRONZE LAYER"]
        SOURCE["HuggingFace Amazon Reviews"]
        STAGE["Snowflake Internal Stage"]
        RAW["RAW_SCHEMA (VARIANT)"]
    end

    subgraph SILVER["SILVER LAYER"]
        subgraph DBT_PIPELINE["dbt Pipeline"]
            STAGING["Staging"]
            TESTS1["Tests"]
            INTERMEDIATE["Intermediate"]
            TESTS2["Final Tests & Quality"]
        end
        DBT["dbt Cloud"]
    end

    subgraph GOLDEN["GOLDEN LAYER"]
        CURATED["CURATED Schema"]
        subgraph MARTS["Data Marts"]
            INSIGHTS["Product Insights"]
            EMBEDDINGS["Review Embeddings"]
            ALERTS["Anomaly Alerts"]
        end
    end

    subgraph AI["AI LAYER"]
        subgraph AGENTS["AI Agents"]
            RAG["RAG Q&A Agent"]
            MONITOR["Monitoring Agent"]
        end
        CORTEX["Snowflake Cortex LLM"]
    end

    subgraph UI["INTERFACE"]
        STREAMLIT["Streamlit Chat UI"]
    end

    AIRFLOW -.->|schedules| SOURCE
    AIRFLOW -.->|triggers| DBT
    AIRFLOW -.->|triggers| MONITOR

    SOURCE --> STAGE
    STAGE --> RAW

    RAW --> STAGING
    STAGING --> TESTS1
    TESTS1 --> INTERMEDIATE
    INTERMEDIATE --> TESTS2
    DBT -.->|orchestrates| DBT_PIPELINE

    TESTS2 --> CURATED
    CURATED --> INSIGHTS
    CURATED --> EMBEDDINGS
    CURATED --> ALERTS

    INSIGHTS --> RAG
    EMBEDDINGS --> RAG
    ALERTS --> MONITOR
    CORTEX -.->|powers| AGENTS

    RAG --> STREAMLIT
    MONITOR --> STREAMLIT
```
| Layer | Technology | Purpose |
|-------|------------|---------|
| Data Source | HuggingFace Datasets | Amazon Reviews extraction |
| File Format | Apache Parquet | Columnar, compressed storage |
| Data Warehouse | Snowflake | Storage + Cortex AI |
| Transformation | dbt | SQL modeling & testing |
| Orchestration | Apache Airflow | Pipeline scheduling |
| AI/ML | Snowflake Cortex | LLM & Embeddings |
## Tech Stack

- Snowflake (Data Warehouse)
- Snowflake Cortex AI
- SQL
- GitHub

## Current Progress

Completed:

- Data ingestion
- Data transformation
- Data validation
- Analytics preparation

Next steps:

- Embeddings
- RAG pipeline
- Insight generation

## Project Log

See: docs/project_log.md
