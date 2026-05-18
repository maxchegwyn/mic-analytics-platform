# mic-analytics-platform

A production data platform built on [Make it Conscious](https://makeitconscious.com) — an inner work resource grounded in Jungian depth psychology, producing guided active imagination exercises on YouTube, coaching, psychological models, and assessments. Each part of the project lives on a different platform: a YouTube channel, two Spotify podcasts, a WordPress site with WooCommerce shop, Stripe, Mailchimp, JotForm, and Google Analytics. Each platform has its own dashboard. None has a view of the whole.

The platform was built to put the whole picture in one place: a data warehouse pulling from every source, transforming raw inputs into a unified set of analytical tables, and serving a single dashboard on top.

---

## Architecture

```
Sources          Ingestion         Warehouse         Transform         BI
────────         ─────────         ─────────         ─────────         ──
YouTube    ──►                                                         Power BI
Mailchimp  ──►   dlt (Python)  ──► BigQuery      ──► dbt Core     ──► (4 tabs)
Stripe     ──►   pipelines         raw layer         13 staging
WooCommerce──►   (GitHub                             15 mart
JotForm    ──►   Actions)          make-it-          models
WordPress  ──►                     conscious
Spotify    ──►                     project
GA4        ──►
```

All ingestion and transformation runs daily via GitHub Actions at 04:00 UTC. The orchestration sequence is: dlt ingestion → dbt run. No Airflow or external orchestrator — GitHub Actions is sufficient at this data volume and a deliberate simplicity choice.

---

## Authentication & Secret Governance

Long-lived service account keys are not used anywhere in this project.

**Workload Identity Federation** authenticates GitHub Actions to Google Cloud without storing credentials. The workflow presents a short-lived OIDC token issued by GitHub; GCP validates it against a configured WIF pool and grants a scoped impersonation token. The token is valid for the duration of the workflow run only.

**GCP Secret Manager** holds all sensitive configuration (API keys, OAuth tokens, database credentials). Secrets are pulled at runtime via `gcloud secrets versions access`. Nothing sensitive is stored in the repository or in GitHub Actions secrets beyond the WIF provider and service account identifiers, which are not themselves sensitive.

**GitHub log masking** — all secret values retrieved at runtime are immediately registered with `::add-mask::` before any further script execution. This prevents accidental exposure in workflow logs even if a downstream step errors verbosely.

---

## Repository Structure

```
mic-analytics-platform/
├── ingestion/                 # dlt pipeline scripts (one per source)
│   ├── youtube.py
│   ├── mailchimp.py
│   ├── stripe_ingestion.py
│   ├── woocommerce.py
│   ├── jotform.py
│   ├── wordpress.py
│   └── spotify.py
├── scripts/
│   └── generate_synthetic_data.py
├── transform/                 # dbt Core project
│   ├── models/
│   │   ├── staging/           # 13 models — one per source table, cleaning only
│   │   └── marts/             # 15 models — cross-source joins, business logic
│   └── dbt_project.yml
├── .github/
│   └── workflows/
│       └── daily_ingestion.yml
├── .env.example
├── .gitattributes
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Data Stack

| Layer | Tool | Notes |
|---|---|---|
| Ingestion | [dlt](https://dlthub.com) (Python) | Schema inference, incremental loading, automatic BigQuery table creation |
| Warehouse | BigQuery (`make-it-conscious` project) | Raw layer untouched; `dbt_prod` dataset holds all transformed tables |
| Transformation | dbt Core 1.11 | Staging/marts separation; `dbt docs` generates lineage automatically |
| Orchestration | GitHub Actions | WIF authentication to GCP; daily cron at 04:00 UTC |
| BI | Power BI | Import mode; single .pbix; daily refresh from `dbt_prod` |

---

## dbt Models

**Staging (13 models)** — cleaning and standardisation only: column renames, type casts, simple filters, deduplication where required. No business logic at this layer.

| Model | Source | Key transforms |
|---|---|---|
| `stg_ga4_events` | `analytics_322691207.events_*` | Page view events unpacked; forward-looking from March 2026 |
| `stg_jotform` | `raw_jotform.submissions_*` | UNION ALL of standard and manual opt submissions; clarity scores extracted via `REGEXP_EXTRACT` |
| `stg_mailchimp_campaigns` | `raw_mailchimp.campaigns` | `status = 'sent'` filter; open/click rates from `__v_double` columns |
| `stg_mailchimp_email_activity` | `raw_mailchimp.email_activity` | Child table left joined to parent on `_dlt_parent_id` |
| `stg_mailchimp_members` | `raw_mailchimp.members` | QUALIFY dedup on email (latest `last_changed`) |
| `stg_spotify_episodes` | `raw_spotify.episodes_*` | UNION ALL of both podcast episode tables; `duration_ms / 60000` to minutes |
| `stg_stripe_charges` | `raw_stripe.charges` | `amount / 100.0`; `paid = true AND refunded = false` filter |
| `stg_video_metadata` | `raw_youtube.raw_video_metadata` | `publish_date` cast to DATE; QUALIFY dedup on `video_id` |
| `stg_video_playlists` | `raw_youtube.raw_video_playlists` | Column rename only |
| `stg_woocommerce_orders` | `raw_woocommerce.orders` + `orders__line_items` | Line item unnest via `_dlt_parent_id`; `total` cast to FLOAT64; `completed` and `processing` statuses only |
| `stg_wordpress_posts` | `raw_wordpress.posts` | `status = 'publish'` filter; `title__rendered` renamed |
| `stg_youtube` | `raw_youtube.raw_video_daily_metrics` | `estimated_minutes_watched / 60` to `watch_hours`; QUALIFY dedup on `video_id, date` |
| `stg_youtube_channel` | `raw_youtube.raw_channel_daily_metrics` | `estimated_minutes_watched / 60` to `watch_hours`; QUALIFY dedup on `date` |

**Marts (15 models)** — cross-source joins and business logic. Materialised as tables in `dbt_prod`.

| Model | Type | Description |
|---|---|---|
| `dim_blog_posts` | Dimension | WordPress published posts: slug, title, URL, publish and modified dates |
| `dim_campaigns` | Dimension | Sent Mailchimp campaigns with open and click rates |
| `dim_customers` | Dimension | Unified customer record joining Mailchimp, JotForm, WooCommerce, and Stripe on email |
| `dim_videos` | Dimension | Video metadata with `content_type` (exercise/other) and `exercise_category` derived from playlist membership |
| `fct_aie_decay_curve` | Fact | Quarterly P25/P50/P75 cumulative watch hours across AIE exercise videos; basis for the decay projection model |
| `fct_blog_performance` | Fact | GA4 page-view sessions per post per day by source/medium (forward-looking from March 2026 export start) |
| `fct_content_calendar` | Fact | Cross-platform publish events unified across YouTube, WordPress, and Spotify |
| `fct_pda_conversion` | Fact | Submission-to-purchase conversion with 24-hour clustering of repeat submissions; `conversion_type` distinguishes immediate, delayed, superseded, and non-converter |
| `fct_pda_takers` | Fact | One row per JotForm submission; carries dominant/auxiliary functions, MiC type names, temperament, conversion window, and test/repeat taker flags |
| `fct_revenue` | Fact | Unified revenue across Stripe (with WooCommerce product join), PayPal WooCommerce orders, and YouTube AdSense |
| `fct_youtube_calendar` | Fact | Daily video metrics joined to `dim_videos` for content type and category filtering |
| `fct_youtube_channel_daily` | Fact | Channel-level daily metrics: views, watch hours, net subscribers, revenue |
| `fct_youtube_daily_wh_by_type` | Fact | Monthly watch hours aggregated by `content_type` (exercise vs other); used for cumulative trend charts |
| `fct_youtube_performance` | Fact | Per-video quarterly watch hours using `DATE_ADD` quarter bucketing; `is_complete_quarter` flag for decay analysis |
| `fct_youtube_video_daily` | Fact | Per-video daily metrics joined to `dim_videos` for content type and category |

---

## Synthetic Data Note

This repository contains real architectural patterns and transformation logic from a live production system. Source data (YouTube metrics, subscriber records, transaction history) is not committed to the repository.

Anyone reproducing this project would need to supply their own API credentials and source data.

---

## Related Work

**[Content Strategy Through Data: A Make it Conscious YouTube Analysis](https://medium.com/@maxchegwyn/content-strategy-through-data-a-make-it-conscious-youtube-analysis-aea066faaf9d)** — a standalone analytical project built on this platform's data, covering category performance, empirically derived decay modelling (0.879 quarterly late-stage decay rate, n=254 videos), per-video lifetime watch-hour projection, and channel-level forecasting.

---

## Local Development

```bash
# Install dbt dependencies
cd transform
pip install dbt-bigquery --break-system-packages

# Run all models
dbt run

# Run a specific model and its downstream dependencies
dbt run --select model_name+

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

A `profiles.yml` pointing to `dbt_dev` (development dataset) is required locally. dbt looks for this at `~/.dbt/profiles.yml` by default — it is not committed to the repository.