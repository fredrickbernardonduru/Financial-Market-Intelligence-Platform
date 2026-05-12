import { NextResponse } from 'next/server'
import { query } from '@/lib/db'
import type { DagRun } from '@/lib/types'

export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    // Get the last 7 calendar days
    const days = await query<{ run_date: string }>(`
      SELECT DISTINCT date::text AS run_date
      FROM (
        SELECT timestamp::date AS date FROM bronze_stock_ticks
        UNION
        SELECT date FROM gold_daily_summary
      ) d
      WHERE date >= CURRENT_DATE - INTERVAL '7 days'
      ORDER BY date DESC
      LIMIT 7
    `)

    // For each day, check which tables have data (tells us which tasks ran)
    const runs: DagRun[] = await Promise.all(
      days.map(async ({ run_date }): Promise<DagRun> => {
        const [b, s, g, a] = await Promise.all([
          query<{ n: string }>(
            `SELECT COUNT(*) AS n FROM bronze_stock_ticks WHERE timestamp::date = $1`,
            [run_date]
          ),
          query<{ n: string }>(
            `SELECT COUNT(*) AS n FROM silver_stock_indicators WHERE timestamp::date = $1`,
            [run_date]
          ),
          query<{ n: string }>(
            `SELECT COUNT(*) AS n FROM gold_daily_summary WHERE date = $1`,
            [run_date]
          ),
          query<{ n: string }>(
            `SELECT COUNT(*) AS n FROM gold_anomalies WHERE detected_at::date = $1`,
            [run_date]
          ),
        ])

        const bronzeOk  = parseInt(b[0]?.n ?? '0') > 0
        const silverOk  = parseInt(s[0]?.n ?? '0') > 0
        const goldOk    = parseInt(g[0]?.n ?? '0') > 0
        const anomalyOk = goldOk // anomaly_detection always runs if gold ran

        const stages = [
          { id: 'E', label: 'Extract',    status: (bronzeOk  ? 'success' : 'failed') as 'success' | 'failed' | 'pending' },
          { id: 'I', label: 'Indicators', status: (silverOk  ? 'success' : bronzeOk ? 'failed' : 'pending') as 'success' | 'failed' | 'pending' },
          { id: 'G', label: 'Gold',       status: (goldOk    ? 'success' : silverOk ? 'failed' : 'pending') as 'success' | 'failed' | 'pending' },
          { id: 'A', label: 'Anomaly',    status: (anomalyOk ? 'success' : goldOk   ? 'failed' : 'pending') as 'success' | 'failed' | 'pending' },
          { id: 'V', label: 'Validate',   status: (anomalyOk ? 'success' : 'pending') as 'success' | 'failed' | 'pending' },
        ]

        const fullySucceeded = stages.every(s => s.status === 'success')

        return {
          run_date,
          stages,
          total_duration_s: null, // Airflow logs not yet integrated
          success: fullySucceeded,
        }
      })
    )

    return NextResponse.json({ runs })
  } catch (err) {
    console.error('[/api/dag-runs]', err)
    return NextResponse.json(
      { error: 'Failed to fetch DAG runs', detail: String(err) },
      { status: 500 }
    )
  }
}
