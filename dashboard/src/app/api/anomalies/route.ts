import { NextResponse } from 'next/server'
import { query } from '@/lib/db'
import type { GoldAnomaly, AnomalyBreakdown, AnomalyType } from '@/lib/types'

export const dynamic = 'force-dynamic'

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const limit  = Math.min(parseInt(searchParams.get('limit') ?? '20'), 100)
    const ticker = searchParams.get('ticker') // optional filter

    // Recent anomalies — mirrors gold_anomalies schema exactly
    const anomalySql = `
      SELECT
        ticker,
        date::text,
        close,
        volume,
        daily_return,
        volatility_7,
        volume_ma_7,
        anomaly_type,
        anomaly_reason,
        detected_at::text AS detected_at
      FROM gold_anomalies
      ${ticker ? 'WHERE ticker = $2' : ''}
      ORDER BY detected_at DESC
      LIMIT $1
    `
    const anomalyParams = ticker ? [limit, ticker] : [limit]

    // Breakdown by type for the donut chart
    const breakdownSql = `
      SELECT anomaly_type AS type, COUNT(*) AS count
      FROM gold_anomalies
      GROUP BY anomaly_type
      ORDER BY count DESC
    `

    const [anomalies, breakdown] = await Promise.all([
      query<GoldAnomaly>(anomalySql, anomalyParams),
      query<{ type: AnomalyType; count: string }>(breakdownSql),
    ])

    return NextResponse.json({
      anomalies,
      breakdown: breakdown.map(
        (b): AnomalyBreakdown => ({ type: b.type, count: parseInt(b.count) })
      ),
    })
  } catch (err) {
    console.error('[/api/anomalies]', err)
    return NextResponse.json(
      { error: 'Failed to fetch anomalies', detail: String(err) },
      { status: 500 }
    )
  }
}
