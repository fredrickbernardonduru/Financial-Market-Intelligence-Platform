import { ReactNode } from 'react'

interface SectionCardProps {
  title: string
  icon?: ReactNode
  children: ReactNode
  className?: string
  action?: ReactNode
}

export function SectionCard({ title, icon, children, className = '', action }: SectionCardProps) {
  return (
    <div className={`bg-zinc-900 border border-zinc-800 rounded-xl p-4 flex flex-col gap-3 ${className}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-medium text-white">
          {icon && <span className="text-zinc-400 w-4 h-4">{icon}</span>}
          {title}
        </div>
        {action}
      </div>
      {children}
    </div>
  )
}
