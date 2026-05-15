'use client'

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts'
import { format, parseISO } from 'date-fns'
import type { PricePoint } from '@/lib/types'

interface PriceChartProps {
  data: PricePoint[]
  height?: number
}

const TICKER_COLORS: Record<string, string> = {
  AAPL: '#34d399',  // emerald
  MSFT: '#60a5fa',  // blue
}
const FALLBACK_COLORS = ['#a78bfa', '#f472b6', '#fb923c', '#facc15']

// Pivot rows → { date, AAPL: 194.3, MSFT: 421.1, ... }
function pivot(rows: PricePoint[]): Record<string, string | number>[] {
  const map: Record<string, Record<string, string | number>> = {}
  for (const r of rows) {
    if (!map[r.date]) map[r.date] = { date: r.date }
    map[r.date][r.ticker] = r.close
  }
  return Object.values(map).sort((a, b) =>
    String(a.date).localeCompare(String(b.date))
  )
}

function tickerColor(ticker: string, index: number): string {
  return TICKER_COLORS[ticker] ?? FALLBACK_COLORS[index % FALLBACK_COLORS.length]
}

export function PriceChart({ data, height = 200 }: PriceChartProps) {
  const pivoted = pivot(data)
  const tickers = [...new Set(data.map(d => d.ticker))]

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={pivoted} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 10, fill: '#71717a' }}
          tickFormatter={v => {
            try { return format(parseISO(String(v)), 'MMM d') } catch { return String(v) }
          }}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tick={{ fontSize: 10, fill: '#71717a' }}
          tickFormatter={v => `$${v}`}
          tickLine={false}
          axisLine={false}
          width={48}
        />
        <Tooltip
          contentStyle={{ background: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
          labelStyle={{ color: '#a1a1aa', marginBottom: 4 }}
          formatter={(v: number, name: string) => [`$${v.toFixed(2)}`, name]}
          labelFormatter={l => {
            try { return format(parseISO(String(l)), 'MMMM d, yyyy') } catch { return l }
          }}
        />
        <Legend wrapperStyle={{ fontSize: 11, color: '#a1a1aa' }} />
        {tickers.map((t, i) => (
          <Line
            key={t}
            type="monotone"
            dataKey={t}
            stroke={tickerColor(t, i)}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4 }}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  )
}
