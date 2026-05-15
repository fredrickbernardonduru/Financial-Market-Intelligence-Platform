'use client'

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from 'recharts'
import { format, parseISO } from 'date-fns'

interface IngestPoint { date: string; ticker: string; row_count: number }

const TICKER_COLORS: Record<string, string> = {
  AAPL: '#34d399',
  MSFT: '#60a5fa',
}
const FALLBACK = ['#a78bfa', '#f472b6']

interface Props { data: IngestPoint[]; height?: number }

function pivot(rows: IngestPoint[]) {
  const map: Record<string, Record<string, string | number>> = {}
  for (const r of rows) {
    if (!map[r.date]) map[r.date] = { date: r.date }
    map[r.date][r.ticker] = (Number(map[r.date][r.ticker] ?? 0)) + r.row_count
  }
  return Object.values(map).sort((a, b) => String(a.date).localeCompare(String(b.date)))
}

export function IngestBarChart({ data, height = 200 }: Props) {
  const pivoted = pivot(data)
  const tickers = [...new Set(data.map(d => d.ticker))]

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={pivoted} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 10, fill: '#71717a' }}
          tickFormatter={v => { try { return format(parseISO(String(v)), 'MMM d') } catch { return v } }}
          tickLine={false} axisLine={false}
        />
        <YAxis
          tick={{ fontSize: 10, fill: '#71717a' }}
          tickLine={false} axisLine={false} width={36}
        />
        <Tooltip
          contentStyle={{ background: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
          labelStyle={{ color: '#a1a1aa', marginBottom: 4 }}
          labelFormatter={l => { try { return format(parseISO(String(l)), 'MMMM d, yyyy') } catch { return l } }}
        />
        <Legend wrapperStyle={{ fontSize: 11, color: '#a1a1aa' }} />
        {tickers.map((t, i) => (
          <Bar key={t} dataKey={t} stackId="a" fill={TICKER_COLORS[t] ?? FALLBACK[i]} radius={i === tickers.length - 1 ? [3, 3, 0, 0] : [0, 0, 0, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}
