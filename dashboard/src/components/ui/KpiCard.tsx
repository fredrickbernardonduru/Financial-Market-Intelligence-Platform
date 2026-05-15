'use client'

import { ReactNode } from 'react'
import { clsx } from 'clsx'

interface KpiCardProps {
  label: string
  value: string | number
  sub?: string
  subVariant?: 'up' | 'down' | 'neutral' | 'warn'
  icon: ReactNode
  loading?: boolean
}

const subColors: Record<string, string> = {
  up:      'text-emerald-500',
  down:    'text-red-500',
  neutral: 'text-zinc-500',
  warn:    'text-amber-500',
}

export function KpiCard({ label, value, sub, subVariant = 'neutral', icon, loading }: KpiCardProps) {
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 flex flex-col gap-1.5">
      <div className="flex items-center gap-2 text-zinc-400 text-xs uppercase tracking-wider">
        <span className="text-zinc-500 w-4 h-4 flex-shrink-0">{icon}</span>
        {label}
      </div>
      {loading ? (
        <div className="h-7 w-20 bg-zinc-800 rounded animate-pulse" />
      ) : (
        <span className="text-2xl font-medium text-white">{value}</span>
      )}
      {sub && (
        <span className={clsx('text-xs', subColors[subVariant])}>{sub}</span>
      )}
    </div>
  )
}
