import React from 'react';
import { FileEdit, Wand2, FileJson } from 'lucide-react';
import { cn } from "@/lib/utils";

export default function SchemaModeTabs({ activeMode, onModeChange }) {
  const modes = [
    {
      id: 'manual',
      label: 'Manual Schema',
      icon: FileEdit,
      description: 'Define columns manually'
    },
    {
      id: 'auto_inference',
      label: 'Auto Inference',
      icon: Wand2,
      description: 'Infer from sample data'
    },
    {
      id: 'single_column',
      label: 'Single Column',
      icon: FileJson,
      description: 'Store raw JSON'
    }
  ];

  return (
    <div className="grid grid-cols-3 gap-3">
      {modes.map(({ id, label, icon: Icon, description }) => (
        <button
          key={id}
          type="button"
          onClick={() => onModeChange(id)}
          className={cn(
            "flex flex-col items-center gap-2 p-4 rounded-xl border-2 transition-all text-center",
            activeMode === id
              ? "border-[#059669] bg-[#D1FAE5]"
              : "border-slate-200 bg-white hover:border-[#059669]/50"
          )}
        >
          <div className={cn(
            "p-2 rounded-lg",
            activeMode === id ? "bg-[#059669] text-white" : "bg-slate-100"
          )}>
            <Icon className="w-5 h-5" />
          </div>
          <div>
            <p className="font-medium text-sm text-[#111827]">{label}</p>
            <p className="text-xs text-slate-500 mt-1">{description}</p>
          </div>
        </button>
      ))}
    </div>
  );
}
