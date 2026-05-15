import { NextResponse } from 'next/server'
import { query } from '@/lib/db'
import type { MetricsResponse } from '@/lib/types'

export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    // Run all count queries in parallel — mirrors validate_pipeline_outputs() in market_ingestion_dag.py
    const [
      bronzeRows,
      silverRows,
      goldRows,
      anomalyRows,
      anomaliesTodayRows,
      lastIngestedRows,
    ] = await Promise.all([
      query<{ count: string }>('SELECT COUNT(*) AS count FROM bronze_stock_ticks'),
      query<{ count: string }>('SELECT COUNT(*) AS count FROM silver_stock_indicators'),
      query<{ count: string }>('SELECT COUNT(*) AS count FROM gold_daily_summary'),
      query<{ count: string }>('SELECT COUNT(*) AS count FROM gold_anomalies'),
      query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM gold_anomalies WHERE detected_at::date = CURRENT_DATE"
      ),
      query<{ last_ts: string | null }>(
        'SELECT MAX(timestamp)::text AS last_ts FROM bronze_stock_ticks'
      ),
    ])

    const bronzeCount = parseInt(bronzeRows[0]?.count ?? '0')
    const silverCount = parseInt(silverRows[0]?.count ?? '0')

    // Validation pass rate = silver / bronze (silver only contains valid records)
    const passRate = bronzeCount > 0
      ? Math.round((silverCount / bronzeCount) * 1000) / 10
      : 0

    const payload: MetricsResponse = {
      bronze_count:        bronzeCount,
      silver_count:        silverCount,
      gold_count:          parseInt(goldRows[0]?.count ?? '0'),
      anomaly_count:       parseInt(anomalyRows[0]?.count ?? '0'),
      anomalies_today:     parseInt(anomaliesTodayRows[0]?.count ?? '0'),
      kafka_topic:         process.env.KAFKA_TOPIC ?? 'stock_ticks',
      last_ingested_at:    lastIngestedRows[0]?.last_ts ?? null,
      validation_pass_rate: passRate,
    }

    return NextResponse.json(payload)
  } catch (err) {
    console.error('[/api/metrics]', err)
    return NextResponse.json(
      { error: 'Failed to fetch metrics', detail: String(err) },
      { status: 500 }
    )
  }
}
