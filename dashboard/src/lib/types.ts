// ─── bronze_stock_ticks ────────────────────────────────────────────
export interface BronzeTick {
  ticker: string
  timestamp: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

// ─── silver_stock_indicators ───────────────────────────────────────
export interface SilverIndicator {
  ticker: string
  timestamp: string
  close: number
  volume: number
  daily_return: number | null
  ma_7: number | null
  ma_20: number | null
  volatility_7: number | null
  volume_ma_7: number | null
}

// ─── gold_daily_summary ────────────────────────────────────────────
export interface GoldSummary {
  ticker: string
  date: string
  close: number
  volume: number
  daily_return: number | null
  ma_7: number | null
  ma_20: number | null
  volatility_7: number | null
  volume_ma_7: number | null
  signal: 'bullish' | 'bearish' | 'neutral' | null
  created_at: string
}

// ─── gold_anomalies ────────────────────────────────────────────────
export type AnomalyType = 'price_spike' | 'price_drop' | 'volume_spike' | 'high_volatility'

export interface GoldAnomaly {
  ticker: string
  date: string
  close: number | null
  volume: number | null
  daily_return: number | null
  volatility_7: number | null
  volume_ma_7: number | null
  anomaly_type: AnomalyType
  anomaly_reason: string
  detected_at: string
}

// ─── API response shapes ───────────────────────────────────────────
export interface MetricsResponse {
  bronze_count: number
  silver_count: number
  gold_count: number
  anomaly_count: number
  anomalies_today: number
  kafka_topic: string
  last_ingested_at: string | null
  validation_pass_rate: number
}

export interface PipelineStage {
  step: number
  task_id: string
  label: string
  status: 'success' | 'running' | 'failed' | 'pending'
  duration_s: number | null
  detail: string
}

export interface PipelineResponse {
  dag_id: string
  schedule: string
  last_run_date: string | null
  stages: PipelineStage[]
}

export interface DagRun {
  run_date: string
  stages: {
    id: string
    label: string
    status: 'success' | 'failed' | 'pending'
  }[]
  total_duration_s: number | null
  success: boolean
}

export interface PricePoint {
  date: string
  ticker: string
  close: number
  daily_return: number | null
  signal: string | null
}

export interface AnomalyBreakdown {
  type: AnomalyType
  count: number
}
