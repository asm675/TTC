# TTC Streaming Pipeline

Modern data engineering project that ingests live TTC subway departures, enriches them with headway analytics, and surfaces the results through an internal API and alerting layer. Everything runs locally so I can prototype quickly and iterate without cloud costs.

## What I Built
- **Real-time ingestion**: a lightweight Python producer (`Producer.py`) polls the public TTC feed and produces structured snapshots to a Kafka-compatible topic.
- **Streaming analytics**: Spark Structured Streaming (`spark_ttc_stream.py`) parses those snapshots, flattens nested route data, and calculates per-route headway metrics including rolling averages, standard deviation, and anomaly flags.
- **Operational data store**: MongoDB persists the raw snapshots, enriched departures, and analytical metrics for inspection and alerting.
- **Observability API**: a FastAPI service (`api/main.py`) exposes health checks and metrics queries, and schedules background jobs that notify a Slack webhook when data gets stale or headways look abnormal.

The goal is to demonstrate end-to-end ownership: ingest, process, store, and monitor a real-time transit dataset using production-grade tooling.

## Architecture
```text
TTC Feed --> Kafka (Redpanda) --> Spark Streaming --> MongoDB
                                           |
                                           v
                                        FastAPI
                                           |
                                           v
                                          Slack
```

- **Kafka / Redpanda**: durable transport layer for streaming snapshots.
- **Spark**: handles schema onboarding, nested JSON explosion, and per-station headway calculations with window functions.
- **MongoDB**: holds three collections (raw snapshots, flattened departures, headway metrics) that power API responses and alert logic.
- **FastAPI + APScheduler**: central status layer that both exposes metrics and runs scheduled checks.
- **Slack**: optional notification channel wired through an incoming webhook.

## Repository Layout
```text
. 
|-- api/                  FastAPI application, Dockerfile, requirements
|-- chk/                  Spark Structured Streaming checkpoints (gitignored)
|-- Producer.py           Kafka producer polling the TTC HTTP API
|-- spark_ttc_stream.py   Spark job that parses, enriches, and persists the stream
|-- docker-compose.yml    Local stack (Redpanda, MongoDB, Spark job, FastAPI API)
|-- .env                  Runtime configuration (gitignored)
`-- README.md
```

## Highlights
- Builds a fully local streaming stack using Redpanda, Spark 3.5, MongoDB, and FastAPI.
- Applies windowed analytics to live transit data, surfacing gap detection via z-scores and configurable thresholds.
- Demonstrates how to orchestrate background tasks inside a FastAPI lifespan using APScheduler.
- Uses Slack webhooks for lightweight alerting, keeping the feedback loop tight during development.
- Container setup (Dockerfile + compose) makes the components portable even though they run on my machine.

## Future Ideas
- Add automated anomaly evaluation using historical baselines beyond rolling windows.
- Package the API as a public-facing dashboard experience.
- Explore lightweight front-end visualizations or embed the metrics into a Streamlit app.
