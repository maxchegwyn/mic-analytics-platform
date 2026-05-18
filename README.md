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
Stripe     ──►   pipelines         raw layer         12 staging
WooCommerce──►   (GitHub                             15 mart           Tableau
JotForm    ──►   Actions)          make-it-          models            (YouTube
WordPress  ──►                     conscious                           lifecycle
Spotify    ──►                     project                             charts)
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
├── transform/                 # dbt Core project
│   ├── models/
│   │   ├── staging/           # 12 models — one per source, cleaning only
│   │   └── marts/             # 15 models — cross-source joins, business logic
│   ├── dbt_project.yml
│   └── profiles.yml           # not committed — local only
├── .github/
│   └── workflows/
│       └── daily_ingestion.yml
├── .gitattributes
├── .gitignore
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

**Staging (12 models)** — one model per source. Cleaning and standardisation only: column renames, type casts, simple filters, deduplication where required. No business logic at this layer.

| Model | Source | Key transforms |
|---|---|---|
| `stg_jotform` | `raw_jotform.submissions_*` | UNION of standard + manual opt submissions |
| `stg_mailchimp_campaigns` | `raw_mailchimp.campaigns` | `status = 'sent'` filter |
| `stg_mailchimp_email_activity` | `raw_mailchimp.email_activity` | Parent/child join on `_dlt_parent_id` |
| `stg_mailchimp_members` | `raw_mailchimp.members` | QUALIFY dedup on email (latest `last_changed`) |
| `stg_spotify_episodes` | `raw_spotify.*` | Episode metadata for both podcasts |
| `stg_stripe_charges` | `raw_stripe.charges` | `amount / 100.0`; `paid = true AND refunded = false` |
| `stg_video_metadata` | `raw_youtube.raw_video_metadata` | `publish_date` cast to DATE |
| `stg_video_playlists` | `raw_youtube.raw_video_playlists` | Filtered to 14 exercise playlists |
| `stg_woocommerce_orders` | `raw_woocommerce.orders` | Line item unnest via `_dlt_parent_id`; total cast to FLOAT64 |
| `stg_wordpress_posts` | `raw_wordpress.posts` | `status = 'publish'` filter |
| `stg_youtube` | `raw_youtube.raw_video_daily_metrics` | `estimated_minutes_watched / 60` → `watch_hours` |
| `stg_youtube_channel` | `raw_youtube.raw_channel_daily_metrics` | Channel-level daily aggregates |

**Marts (15 models)** — cross-source joins and business logic. Materialised as tables in `dbt_prod`.

| Model | Type | Description |
|---|---|---|
| `dim_blog_posts` | Dimension | WordPress posts with category tags |
| `dim_campaigns` | Dimension | Mailchimp campaigns with send and engagement metrics |
| `dim_customers` | Dimension | Unified customer record across Stripe, WooCommerce, Mailchimp, JotForm |
| `dim_videos` | Dimension | Video metadata with playlist and category classifications |
| `fct_aie_decay_curve` | Fact | Per-video quarterly decay data for the projection model |
| `fct_blog_performance` | Fact | Post-level GA4 engagement (forward-looking from export start) |
| `fct_content_calendar` | Fact | Cross-platform publish events: YouTube, blog, podcast |
| `fct_pda_conversion` | Fact | PDA submission to purchase linkage; conversion type and lead time |
| `fct_pda_takers` | Fact | One row per PDA submission; superseded flag for retakers |
| `fct_revenue` | Fact | Unified revenue: WooCommerce, Stripe (Hestia), YouTube AdSense |
| `fct_youtube_calendar` | Fact | Video publish events joined to channel performance |
| `fct_youtube_channel_daily` | Fact | Channel-level daily metrics |
| `fct_youtube_daily_wh_by_type` | Fact | Watch hours by content category, daily |
| `fct_youtube_performance` | Fact | Per-video quarterly performance with decay projections |
| `fct_youtube_video_daily` | Fact | Per-video daily metrics |

---

## Synthetic Data Note

This repository uses real architectural patterns and transformation logic from a live production system. Source data (YouTube metrics, subscriber records, transaction history) is not committed to the repository. The `yt_timeseries_v3` CSV used in the YouTube capstone analysis contains real aggregate metrics but no personally identifiable information.

Anyone reproducing this project would need to supply their own API credentials and source data.

---

## Related Work

**YouTube Content Strategy Capstone** — a standalone analytical project built on this platform's data, covering category performance, empirically derived decay modelling (0.879 quarterly late-stage decay rate, n=254 videos), per-video lifetime watch-hour projection, and channel-level forecasting. Published at [makeitconscious.com](https://makeitconscious.com) and Medium.

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

A `profiles.yml` pointing to `dbt_dev` (development dataset) is required locally. It is not committed to the repository.