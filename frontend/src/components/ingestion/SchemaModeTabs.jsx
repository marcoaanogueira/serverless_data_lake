import React from 'react';
import { FileEdit, Wand2, FileJson, Sparkles } from 'lucide-react';
import { cn } from "@/lib/utils";

const modeConfig = {
  manual: {
    label: 'Manual Schema',
    icon: FileEdit,
    description: 'Define columns manually',
    color: 'mint',
    bgActive: 'bg-[#D4F5E6]',
    borderActive: 'border-[#7DD3B0]',
    iconBg: 'bg-[#A8E6CF]',
    iconBgActive: 'bg-[#7DD3B0]',
  },
  auto_inference: {
    label: 'Auto Inference',
    icon: Wand2,
    description: 'Infer from sample payload',
    color: 'yellow',
    bgActive: 'bg-[#FFF9DB]',
    borderActive: 'border-[#FBBF24]',
    iconBg: 'bg-[#FFF3B0]',
    iconBgActive: 'bg-[#FBBF24]',
  },
  single_column: {
    label: 'Single Column',
    icon: FileJson,
    description: 'Store raw JSON',
    color: 'lilac',
    bgActive: 'bg-[#DDD6FE]',
    borderActive: 'border-[#A78BFA]',
    iconBg: 'bg-[#C4B5FD]',
    iconBgActive: 'bg-[#A78BFA]',
  }
};

export default function SchemaModeTabs({ activeMode, onModeChange }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      {Object.entries(modeConfig).map(([id, config]) => {
        const Icon = config.icon;
        const isActive = activeMode === id;

        return (
          <button
            key={id}
            type="button"
            onClick={() => onModeChange(id)}
            className={cn(
              "relative flex flex-col items-center gap-3 p-4 rounded-xl border-2 transition-all text-center",
              isActive
                ? `${config.bgActive} ${config.borderActive}`
                : "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50"
            )}
            style={isActive ? { boxShadow: '3px 4px 0 rgba(100, 116, 139, 0.1)' } : {}}
          >
            {/* Sparkle for auto inference */}
            {id === 'auto_inference' && isActive && (
              <Sparkles className="absolute -top-2 -right-2 w-5 h-5 text-[#FBBF24] animate-sparkle" />
            )}

            <div className={cn(
              "p-2.5 rounded-xl transition-colors",
              isActive ? `${config.iconBgActive} text-white` : config.iconBg
            )}>
              <Icon className="w-5 h-5" />
            </div>
            <div>
              <p className={cn(
                "font-semibold text-sm",
                isActive ? "text-slate-700" : "text-slate-600"
              )}>
                {config.label}
              </p>
              <p className="text-xs text-slate-400 mt-1">{config.description}</p>
            </div>

            {/* Hand-drawn check mark for active */}
            {isActive && (
              <svg className="absolute top-2 right-2 w-5 h-5" viewBox="0 0 20 20">
                <path
                  d="M4 10 L8 14 L16 6"
                  stroke="#059669"
                  strokeWidth="2.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  fill="none"
                />
              </svg>
            )}
          </button>
        );
      })}
    </div>
  );
}
