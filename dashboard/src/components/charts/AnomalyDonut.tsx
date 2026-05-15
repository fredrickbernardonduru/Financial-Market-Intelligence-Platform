'use client'

import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts'
import type { AnomalyBreakdown } from '@/lib/types'

const TYPE_COLORS: Record<string, string> = {
  high_volatility: '#f59e0b',
  price_spike:     '#ef4444',
  price_drop:      '#f87171',
  volume_spike:    '#3b82f6',
}

interface Props { data: AnomalyBreakdown[]; height?: number }

export function AnomalyDonut({ data, height = 180 }: Props) {
  const total = data.reduce((s, d) => s + d.count, 0)

  return (
    <div className="flex gap-4 items-center">
      <ResponsiveContainer width="50%" height={height}>
        <PieChart>
          <Pie
            data={data}
            cx="50%" cy="50%"
            innerRadius="55%" outerRadius="80%"
            dataKey="count"
            nameKey="type"
            paddingAngle={2}
          >
            {data.map((entry) => (
              <Cell key={entry.type} fill={TYPE_COLORS[entry.type] ?? '#71717a'} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{ background: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
            formatter={(v: number, name: string) => [v, name.replace(/_/g, ' ')]}
          />
        </PieChart>
      </ResponsiveContainer>

      <div className="flex flex-col gap-2 flex-1">
        <p className="text-2xl font-medium text-white">{total}</p>
        <p className="text-xs text-zinc-500 -mt-1">total anomalies</p>
        {data.map(d => (
          <div key={d.type} className="flex items-center gap-2 text-xs">
            <span
              className="w-2 h-2 rounded-sm flex-shrink-0"
              style={{ background: TYPE_COLORS[d.type] ?? '#71717a' }}
            />
            <span className="text-zinc-400 capitalize flex-1">{d.type.replace(/_/g, ' ')}</span>
            <span className="text-zinc-300 tabular-nums">{d.count}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
