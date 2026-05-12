import { clsx } from 'clsx'
import type { AnomalyType } from '@/lib/types'

type Variant = 'success' | 'danger' | 'warning' | 'info' | 'neutral'

const variantClasses: Record<Variant, string> = {
  success: 'bg-emerald-950 text-emerald-400 border-emerald-800',
  danger:  'bg-red-950 text-red-400 border-red-800',
  warning: 'bg-amber-950 text-amber-400 border-amber-800',
  info:    'bg-blue-950 text-blue-400 border-blue-800',
  neutral: 'bg-zinc-800 text-zinc-400 border-zinc-700',
}

export const anomalyVariant: Record<AnomalyType, Variant> = {
  price_spike:    'danger',
  price_drop:     'danger',
  volume_spike:   'info',
  high_volatility: 'warning',
}

export function StatusBadge({ label, variant }: { label: string; variant: Variant }) {
  return (
    <span className={clsx(
      'text-[10px] font-medium px-2 py-0.5 rounded-full border',
      variantClasses[variant]
    )}>
      {label}
    </span>
  )
}

// Stage status dot
export function StageDot({ status }: { status: 'success' | 'running' | 'failed' | 'pending' }) {
  const cls = {
    success: 'bg-emerald-500',
    running: 'bg-blue-500 animate-pulse',
    failed:  'bg-red-500',
    pending: 'bg-zinc-600',
  }[status]
  return <span className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${cls}`} />
}
