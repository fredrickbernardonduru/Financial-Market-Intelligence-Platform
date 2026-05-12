import { NextResponse } from 'next/server'
import { query } from '@/lib/db'
import type { PricePoint } from '@/lib/types'

export const dynamic = 'force-dynamic'

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const days = Math.min(parseInt(searchParams.get('days') ?? '30'), 90)

    // Fetch close prices, returns, and signals for all tickers
    // Sourced from gold_daily_summary (produced by gold_aggregations.py)
    const rows = await query<PricePoint>(`
      SELECT
        date::text AS date,
        ticker,
        close,
        daily_return,
        signal
      FROM gold_daily_summary
      WHERE date >= CURRENT_DATE - INTERVAL '${days} days'
      ORDER BY date ASC, ticker ASC
    `)

    // Ingest volume per day — for the bar chart
    const ingestVolume = await query<{ date: string; ticker: string; row_count: number }>(`
      SELECT
        timestamp::date::text AS date,
        ticker,
        COUNT(*) AS row_count
      FROM bronze_stock_ticks
      WHERE timestamp >= NOW() - INTERVAL '${days} days'
      GROUP BY date, ticker
      ORDER BY date ASC, ticker ASC
    `)

    return NextResponse.json({ prices: rows, ingest_volume: ingestVolume })
  } catch (err) {
    console.error('[/api/prices]', err)
    return NextResponse.json(
      { error: 'Failed to fetch prices', detail: String(err) },
      { status: 500 }
    )
  }
}
