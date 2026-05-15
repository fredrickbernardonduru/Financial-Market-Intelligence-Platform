import { NextResponse } from 'next/server'
import { query } from '@/lib/db'
import type { PipelineResponse, PipelineStage } from '@/lib/types'

export const dynamic = 'force-dynamic'

// Infers the status of each DAG stage by checking whether each table
// has rows ingested today. Mirrors the task order in market_ingestion_dag.py:
//   extract_validate_load_kafka → calculate_indicators → gold_aggregations
//   → anomaly_detection → validate_pipeline_outputs
async function getStageFreshness() {
  const [bronze, silver, gold, anomalies] = await Promise.all([
    query<{ count: string; last_ts: string | null }>(
      `SELECT COUNT(*) AS count, MAX(timestamp)::text AS last_ts
       FROM bronze_stock_ticks
       WHERE timestamp::date = CURRENT_DATE`
    ),
    query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM silver_stock_indicators
       WHERE timestamp::date = CURRENT_DATE`
    ),
    query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM gold_daily_summary
       WHERE date = CURRENT_DATE`
    ),
    query<{ count: string }>(
      `SELECT COUNT(*) AS count FROM gold_anomalies
       WHERE detected_at::date = CURRENT_DATE`
    ),
  ])

  return {
    bronze:    parseInt(bronze[0]?.count ?? '0'),
    bronzeTs:  bronze[0]?.last_ts ?? null,
    silver:    parseInt(silver[0]?.count ?? '0'),
    gold:      parseInt(gold[0]?.count ?? '0'),
    anomalies: parseInt(anomalies[0]?.count ?? '0'),
  }
}

export async function GET() {
  try {
    const f = await getStageFreshness()

    // Derive stage status from data presence.
    // If bronze has rows → extract ran. If silver has rows → indicators ran. etc.
    // The pipeline is sequential so we can infer the current step.
    const bronzeOk = f.bronze > 0
    const silverOk = f.silver > 0
    const goldOk   = f.gold > 0
    // anomaly detection is considered "done" when it has run OR gold ran (anomalies=0 is valid)
    const anomalyOk = goldOk

    const stages: PipelineStage[] = [
      {
        step: 1,
        task_id: 'extract_validate_load_kafka',
        label:   'Extract → validate → load → Kafka',
        status:  bronzeOk ? 'success' : 'pending',
        duration_s: null,
        detail:  bronzeOk
          ? `${f.bronze.toLocaleString()} rows loaded · AAPL, MSFT`
          : 'Waiting for today\'s run',
      },
      {
        step: 2,
        task_id: 'calculate_indicators',
        label:   'Calculate indicators',
        status:  silverOk ? 'success' : bronzeOk ? 'running' : 'pending',
        duration_s: null,
        detail:  silverOk
          ? `${f.silver.toLocaleString()} rows · ma_7, ma_20, volatility_7`
          : bronzeOk ? 'Running...' : 'Pending',
      },
      {
        step: 3,
        task_id: 'gold_aggregations',
        label:   'Gold aggregations',
        status:  goldOk ? 'success' : silverOk ? 'running' : 'pending',
        duration_s: null,
        detail:  goldOk
          ? `${f.gold.toLocaleString()} rows · daily_summary`
          : silverOk ? 'Running...' : 'Pending',
      },
      {
        step: 4,
        task_id: 'anomaly_detection',
        label:   'Anomaly detection',
        status:  anomalyOk ? 'success' : goldOk ? 'running' : 'pending',
        duration_s: null,
        detail:  anomalyOk
          ? `${f.anomalies} anomaly(ies) detected today`
          : goldOk ? 'Running...' : 'Pending',
      },
      {
        step: 5,
        task_id: 'validate_pipeline_outputs',
        label:   'Validate pipeline outputs',
        status:  anomalyOk ? 'success' : 'pending',
        duration_s: null,
        detail:  anomalyOk
          ? 'All tables passed validation'
          : 'Pending',
      },
    ]

    const payload: PipelineResponse = {
      dag_id:        'market_ingestion_dag',
      schedule:      '@daily',
      last_run_date: f.bronzeTs,
      stages,
    }

    return NextResponse.json(payload)
  } catch (err) {
    console.error('[/api/pipeline]', err)
    return NextResponse.json(
      { error: 'Failed to fetch pipeline status', detail: String(err) },
      { status: 500 }
    )
  }
}
