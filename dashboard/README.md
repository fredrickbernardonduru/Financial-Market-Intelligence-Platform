# Market Intelligence Platform — Observability Dashboard

A Next.js 14 observability dashboard that connects directly to your
PostgreSQL database and surfaces live metrics from all four pipeline layers.

## Project structure

```
dashboard/
├── src/
│   ├── app/
│   │   ├── api/
│   │   │   ├── metrics/route.ts       ← KPI counts from all tables
│   │   │   ├── pipeline/route.ts      ← Live DAG stage status
│   │   │   ├── anomalies/route.ts     ← gold_anomalies table
│   │   │   ├── prices/route.ts        ← gold_daily_summary close prices
│   │   │   └── dag-runs/route.ts      ← 7-day run history
│   │   ├── dashboard/
│   │   │   └── page.tsx               ← Main dashboard UI
│   │   ├── layout.tsx
│   │   └── globals.css
│   ├── components/
│   │   ├── charts/
│   │   │   ├── PriceChart.tsx         ← Recharts line chart (AAPL, MSFT)
│   │   │   ├── IngestBarChart.tsx     ← Stacked bar chart (bronze volume)
│   │   │   └── AnomalyDonut.tsx       ← Donut chart (anomaly breakdown)
│   │   └── ui/
│   │       ├── KpiCard.tsx
│   │       ├── SectionCard.tsx
│   │       └── StatusBadge.tsx
│   └── lib/
│       ├── db.ts                      ← pg Pool, mirrors etl/load.py
│       └── types.ts                   ← TypeScript interfaces for all tables
└── .env.local                         ← Your DB credentials (never commit)
```

## Where to place this in your project

Place the `dashboard/` folder inside your project root:

```
Financial Market Intelligence Platform/
├── airflow/
├── etl/
├── sql/
├── docker/
├── dashboard/          ← add here
│   ├── src/
│   ├── package.json
│   └── ...
└── ...
```

## Setup

### 1. Install dependencies

```bash
cd dashboard
npm install
```

### 2. Configure .env.local

Copy your credentials from the project root `.env`:

```env
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=financial_market_intelligence
DB_USER=postgres
DB_PASSWORD=your_password_here

KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=stock_ticks

NEXT_PUBLIC_REFRESH_INTERVAL=30000
```

> ⚠️ Never commit `.env.local` to git. It's already in `.gitignore` by Next.js convention.

### 3. Make sure your pipeline containers are running

```bash
# From the docker/ directory
docker-compose up -d
```

The dashboard needs PostgreSQL running and at least one pipeline run completed
so the tables have data.

### 4. Run the dashboard

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) — it redirects to `/dashboard`.

### 5. Production build

```bash
npm run build
npm start
```

Or add to your `docker-compose.yml`:

```yaml
  dashboard:
    build:
      context: ./dashboard
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      DB_HOST: postgres
      DB_PORT: 5432
      DB_NAME: ${POSTGRES_DB}
      DB_USER: ${POSTGRES_USER}
      DB_PASSWORD: ${POSTGRES_PASSWORD}
      KAFKA_TOPIC: stock_ticks
    depends_on:
      - postgres
```

## What each API route queries

| Route | Tables queried | Purpose |
|---|---|---|
| `/api/metrics` | all 4 tables | KPI counts, pass rate, last ingest time |
| `/api/pipeline` | all 4 tables | Infers stage status from today's data presence |
| `/api/anomalies` | `gold_anomalies` | Recent anomalies + type breakdown |
| `/api/prices` | `gold_daily_summary`, `bronze_stock_ticks` | Close prices + ingest volume |
| `/api/dag-runs` | all 4 tables | 7-day run history by checking per-day row counts |

## Dashboard auto-refresh

The dashboard polls all endpoints every 30 seconds by default.
Change `NEXT_PUBLIC_REFRESH_INTERVAL` in `.env.local` (value in ms).
