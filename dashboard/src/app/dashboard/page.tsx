'use client'

import { useEffect, useState, useCallback } from 'react'
import { format } from 'date-fns'
import {
  Database, Filter, Trophy, AlertTriangle,
  Radio, GitBranch, RefreshCw, Activity,
  Calendar, TableProperties, TrendingUp,
} from 'lucide-react'

import { KpiCard } from '@/components/ui/KpiCard'
import { SectionCard } from '@/components/ui/SectionCard'
import { StatusBadge, StageDot, anomalyVariant } from '@/components/ui/StatusBadge'
import { PriceChart } from '@/components/charts/PriceChart'
import { IngestBarChart } from '@/components/charts/IngestBarChart'
import { AnomalyDonut } from '@/components/charts/AnomalyDonut'

import type {
  MetricsResponse, PipelineResponse, GoldAnomaly,
  AnomalyBreakdown, PricePoint, DagRun,
} from '@/lib/types'

const REFRESH_MS = parseInt(process.env.NEXT_PUBLIC_REFRESH_INTERVAL ?? '30000')

// ─── helpers ──────────────────────────────────────────────────────────────────
function fmtDate(iso: string | null) {
  if (!iso) return '—'
  try { return format(new Date(iso), 'dd MMM yyyy, HH:mm') } catch { return iso }
}

function fmtNum(n: number) {
  return n.toLocaleString()
}

const stageClasses = {
  success: 'border-emerald-800 bg-emerald-950/40',
  running: 'border-blue-800 bg-blue-950/40',
  failed:  'border-red-800 bg-red-950/40',
  pending: 'border-zinc-800 bg-zinc-900/40',
}
const stageLabelClasses = {
  success: 'text-emerald-400',
  running: 'text-blue-400',
  failed:  'text-red-400',
  pending: 'text-zinc-500',
}

const dagBlockClasses = {
  success: 'bg-emerald-900 text-emerald-300',
  failed:  'bg-red-900 text-red-300',
  pending: 'bg-zinc-800 text-zinc-500',
}

// ─── component ────────────────────────────────────────────────────────────────
export default function DashboardPage() {
  const [metrics, setMetrics]   = useState<MetricsResponse | null>(null)
  const [pipeline, setPipeline] = useState<PipelineResponse | null>(null)
  const [anomalies, setAnomalies] = useState<GoldAnomaly[]>([])
  const [breakdown, setBreakdown] = useState<AnomalyBreakdown[]>([])
  const [prices, setPrices]     = useState<PricePoint[]>([])
  const [ingestVol, setIngestVol] = useState<{ date: string; ticker: string; row_count: number }[]>([])
  const [dagRuns, setDagRuns]   = useState<DagRun[]>([])
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState<string | null>(null)

  const fetchAll = useCallback(async () => {
    try {
      const [mRes, pRes, aRes, prRes, dRes] = await Promise.all([
        fetch('/api/metrics'),
        fetch('/api/pipeline'),
        fetch('/api/anomalies?limit=10'),
        fetch('/api/prices?days=14'),
        fetch('/api/dag-runs'),
      ])

      if (!mRes.ok || !pRes.ok || !aRes.ok || !prRes.ok || !dRes.ok) {
        throw new Error('One or more API endpoints returned an error')
      }

      const [m, p, a, pr, d] = await Promise.all([
        mRes.json(), pRes.json(), aRes.json(), prRes.json(), dRes.json(),
      ])

      setMetrics(m)
      setPipeline(p)
      setAnomalies(a.anomalies ?? [])
      setBreakdown(a.breakdown ?? [])
      setPrices(pr.prices ?? [])
      setIngestVol(pr.ingest_volume ?? [])
      setDagRuns(d.runs ?? [])
      setLastRefresh(new Date())
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchAll()
    const id = setInterval(fetchAll, REFRESH_MS)
    return () => clearInterval(id)
  }, [fetchAll])

  // Latest close per ticker from prices
  const latestPrices: Record<string, { close: number; daily_return: number | null }> = {}
  for (const p of prices) {
    latestPrices[p.ticker] = { close: p.close, daily_return: p.daily_return }
  }

  return (
    <div className="min-h-screen bg-zinc-950 p-4 md:p-6 flex flex-col gap-4 max-w-screen-xl mx-auto">

      {/* ── top bar ─────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between border-b border-zinc-800 pb-4">
        <div className="flex items-center gap-3">
          <Activity className="text-emerald-400" size={20} />
          <h1 className="text-base font-medium text-white">Platform observability</h1>
          <span className="flex items-center gap-1.5 text-[11px] bg-emerald-950 text-emerald-400 border border-emerald-800 px-2 py-0.5 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
            Live
          </span>
        </div>
        <div className="flex items-center gap-3">
          {lastRefresh && (
            <span className="text-xs text-zinc-500">
              Updated {format(lastRefresh, 'HH:mm:ss')}
            </span>
          )}
          <button
            onClick={fetchAll}
            className="flex items-center gap-1.5 text-xs text-zinc-400 border border-zinc-700 rounded-lg px-3 py-1.5 hover:bg-zinc-800 transition-colors"
          >
            <RefreshCw size={12} />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-950 border border-red-800 text-red-300 text-sm rounded-xl px-4 py-3">
          ⚠ Could not connect to the database: {error}
          <br />
          <span className="text-red-500 text-xs">Make sure your PostgreSQL container is running and .env.local is configured.</span>
        </div>
      )}

      {/* ── KPI row ─────────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
        <KpiCard
          label="Bronze rows"
          value={metrics ? fmtNum(metrics.bronze_count) : '—'}
          sub={metrics ? `last ingest ${fmtDate(metrics.last_ingested_at)}` : undefined}
          subVariant="neutral"
          icon={<Database size={14} />}
          loading={loading}
        />
        <KpiCard
          label="Silver rows"
          value={metrics ? fmtNum(metrics.silver_count) : '—'}
          sub={metrics ? `${metrics.validation_pass_rate}% pass rate` : undefined}
          subVariant="neutral"
          icon={<Filter size={14} />}
          loading={loading}
        />
        <KpiCard
          label="Gold rows"
          value={metrics ? fmtNum(metrics.gold_count) : '—'}
          sub="gold_daily_summary"
          subVariant="neutral"
          icon={<Trophy size={14} />}
          loading={loading}
        />
        <KpiCard
          label="Anomalies"
          value={metrics ? fmtNum(metrics.anomaly_count) : '—'}
          sub={metrics ? `${metrics.anomalies_today} detected today` : undefined}
          subVariant={metrics && metrics.anomalies_today > 0 ? 'warn' : 'neutral'}
          icon={<AlertTriangle size={14} />}
          loading={loading}
        />
        <KpiCard
          label="Kafka topic"
          value={metrics?.kafka_topic ?? '—'}
          sub="stock_ticks"
          subVariant="neutral"
          icon={<Radio size={14} />}
          loading={loading}
        />
      </div>

      {/* ── row 2: pipeline + infrastructure ────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        {/* Pipeline stages */}
        <div className="lg:col-span-2">
          <SectionCard
            title="market_ingestion_dag"
            icon={<GitBranch size={14} />}
            action={
              pipeline?.last_run_date ? (
                <span className="text-xs text-zinc-500">{fmtDate(pipeline.last_run_date)}</span>
              ) : null
            }
          >
            <div className="flex flex-col gap-2">
              {(pipeline?.stages ?? Array(5).fill(null)).map((stage, i) => {
                if (!stage) return (
                  <div key={i} className="h-12 bg-zinc-800 rounded-lg animate-pulse" />
                )
                return (
                  <div
                    key={stage.task_id}
                    className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border ${stageClasses[stage.status]}`}
                  >
                    <span className="text-xs font-mono text-zinc-600 w-4 flex-shrink-0">{stage.step}</span>
                    <StageDot status={stage.status} />
                    <div className="flex-1 min-w-0">
                      <p className={`text-xs font-medium font-mono ${stageLabelClasses[stage.status]}`}>
                        {stage.task_id}
                      </p>
                      <p className="text-[11px] text-zinc-500 truncate">{stage.detail}</p>
                    </div>
                  </div>
                )
              })}
            </div>
          </SectionCard>
        </div>

        {/* DAG run history */}
        <SectionCard title="DAG run history" icon={<Calendar size={14} />}>
          {loading ? (
            <div className="flex flex-col gap-2">
              {Array(6).fill(null).map((_, i) => (
                <div key={i} className="h-8 bg-zinc-800 rounded animate-pulse" />
              ))}
            </div>
          ) : (
            <>
              <div className="flex flex-col gap-2">
                {dagRuns.map(run => (
                  <div key={run.run_date} className="flex items-center gap-2">
                    <span className="text-[11px] text-zinc-500 w-14 flex-shrink-0 font-mono">
                      {run.run_date.slice(5)} {/* MM-DD */}
                    </span>
                    <div className="flex gap-1 flex-1">
                      {run.stages.map(s => (
                        <div
                          key={s.id}
                          title={`${s.label}: ${s.status}`}
                          className={`flex-1 h-5 rounded-sm flex items-center justify-center text-[9px] font-bold ${dagBlockClasses[s.status]}`}
                        >
                          {s.id}
                        </div>
                      ))}
                    </div>
                    <span className={`text-[11px] w-8 text-right flex-shrink-0 ${run.success ? 'text-zinc-500' : 'text-red-400'}`}>
                      {run.success ? '✓' : '✗'}
                    </span>
                  </div>
                ))}
              </div>
              <div className="flex gap-3 mt-1 text-[10px] text-zinc-500">
                <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-emerald-900" />success</span>
                <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-red-900" />failed</span>
                <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm bg-zinc-800" />pending</span>
              </div>
            </>
          )}
        </SectionCard>

      </div>

      {/* ── row 3: charts ───────────────────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <SectionCard title="Close prices — last 14 days" icon={<TrendingUp size={14} />} className="lg:col-span-2">
          {loading ? (
            <div className="h-48 bg-zinc-800 rounded-lg animate-pulse" />
          ) : prices.length > 0 ? (
            <PriceChart data={prices} height={200} />
          ) : (
            <EmptyState msg="No price data yet — run the pipeline." />
          )}
        </SectionCard>

        <SectionCard title="Anomaly breakdown" icon={<AlertTriangle size={14} />}>
          {loading ? (
            <div className="h-48 bg-zinc-800 rounded-lg animate-pulse" />
          ) : breakdown.length > 0 ? (
            <AnomalyDonut data={breakdown} />
          ) : (
            <EmptyState msg="No anomalies detected." />
          )}
        </SectionCard>

      </div>

      {/* ── row 4: ingestion volume + latest prices ──────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        <SectionCard title="Records ingested — bronze_stock_ticks" icon={<Database size={14} />} className="lg:col-span-2">
          {loading ? (
            <div className="h-48 bg-zinc-800 rounded-lg animate-pulse" />
          ) : ingestVol.length > 0 ? (
            <IngestBarChart data={ingestVol} height={200} />
          ) : (
            <EmptyState msg="No ingest data yet." />
          )}
        </SectionCard>

        <SectionCard title="Latest closes" icon={<TrendingUp size={14} />}>
          {loading ? (
            <div className="flex flex-col gap-2">
              {[1,2].map(i => <div key={i} className="h-10 bg-zinc-800 rounded animate-pulse" />)}
            </div>
          ) : Object.keys(latestPrices).length > 0 ? (
            <div className="flex flex-col divide-y divide-zinc-800">
              {Object.entries(latestPrices).map(([ticker, v]) => (
                <div key={ticker} className="flex items-center justify-between py-2.5">
                  <span className="text-sm font-medium text-white">{ticker}</span>
                  <span className="text-sm text-white">${v.close.toFixed(2)}</span>
                  {v.daily_return !== null ? (
                    <span className={`text-xs font-medium tabular-nums ${v.daily_return >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {v.daily_return >= 0 ? '+' : ''}{(v.daily_return * 100).toFixed(2)}%
                    </span>
                  ) : (
                    <span className="text-xs text-zinc-500">—</span>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <EmptyState msg="No price data." />
          )}
        </SectionCard>

      </div>

      {/* ── row 5: anomaly table ─────────────────────────────────────── */}
      <SectionCard title="Recent anomalies — gold_anomalies" icon={<TableProperties size={14} />}>
        {loading ? (
          <div className="h-40 bg-zinc-800 rounded-lg animate-pulse" />
        ) : anomalies.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-zinc-800">
                  {['Ticker', 'Date', 'Type', 'Reason', 'Close', 'Return', 'Volatility', 'Detected'].map(h => (
                    <th key={h} className="text-left text-zinc-500 font-medium uppercase tracking-wider pb-2 pr-4 whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800/50">
                {anomalies.map((a, i) => (
                  <tr key={i} className="hover:bg-zinc-800/30 transition-colors">
                    <td className="py-2.5 pr-4 font-medium text-white">{a.ticker}</td>
                    <td className="py-2.5 pr-4 text-zinc-400 font-mono">{a.date}</td>
                    <td className="py-2.5 pr-4">
                      <StatusBadge label={a.anomaly_type.replace(/_/g, ' ')} variant={anomalyVariant[a.anomaly_type]} />
                    </td>
                    <td className="py-2.5 pr-4 text-zinc-400 max-w-xs truncate">{a.anomaly_reason}</td>
                    <td className="py-2.5 pr-4 text-zinc-300 tabular-nums">
                      {a.close != null ? `$${a.close.toFixed(2)}` : '—'}
                    </td>
                    <td className={`py-2.5 pr-4 tabular-nums font-medium ${
                      a.daily_return == null ? 'text-zinc-500' :
                      a.daily_return >= 0 ? 'text-emerald-400' : 'text-red-400'
                    }`}>
                      {a.daily_return != null
                        ? `${a.daily_return >= 0 ? '+' : ''}${(a.daily_return * 100).toFixed(2)}%`
                        : '—'}
                    </td>
                    <td className="py-2.5 pr-4 text-zinc-500 tabular-nums">
                      {a.volatility_7 != null ? (a.volatility_7 * 100).toFixed(3) + '%' : '—'}
                    </td>
                    <td className="py-2.5 text-zinc-500 font-mono whitespace-nowrap">{fmtDate(a.detected_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState msg="No anomalies in gold_anomalies yet — run the pipeline." />
        )}
      </SectionCard>

    </div>
  )
}

function EmptyState({ msg }: { msg: string }) {
  return (
    <div className="flex items-center justify-center py-10 text-zinc-600 text-sm">
      {msg}
    </div>
  )
}
