import React from 'react';
import { FileEdit, Wand2, FileJson } from 'lucide-react';
import { cn } from "@/lib/utils";

const modes = [
  {
    id: 'manual',
    label: 'Manual',
    icon: FileEdit,
    description: 'Define columns',
    color: 'mint',
    bg: 'bg-[#A8E6CF]',
    activeBg: 'bg-[#6BCF9F]',
  },
  {
    id: 'auto_inference',
    label: 'Auto',
    icon: Wand2,
    description: 'Infer schema',
    color: 'lilac',
    bg: 'bg-[#C4B5FD]',
    activeBg: 'bg-[#A78BFA]',
  },
  {
    id: 'single_column',
    label: 'JSON',
    icon: FileJson,
    description: 'Raw storage',
    color: 'peach',
    bg: 'bg-[#FECACA]',
    activeBg: 'bg-[#FCA5A5]',
  }
];

export default function SchemaModeTabs({ activeMode, onModeChange }) {
  return (
    <div className="grid grid-cols-3 gap-3">
      {modes.map(({ id, label, icon: Icon, description, bg, activeBg }) => {
        const isActive = activeMode === id;

        return (
          <button
            key={id}
            type="button"
            onClick={() => onModeChange(id)}
            className={cn(
              "flex flex-col items-center gap-2 p-4 rounded-2xl border-2 transition-all",
              isActive
                ? `${bg} border-gray-900`
                : "bg-white border-gray-200 hover:border-gray-300"
            )}
            style={isActive ? { boxShadow: '4px 4px 0 rgba(0,0,0,0.15)' } : {}}
          >
            <div className={cn(
              "w-10 h-10 rounded-xl flex items-center justify-center transition-colors",
              isActive ? `${activeBg} text-white` : "bg-gray-100 text-gray-500"
            )}>
              <Icon className="w-5 h-5" />
            </div>
            <div className="text-center">
              <p className={cn(
                "font-bold text-sm",
                isActive ? "text-gray-900" : "text-gray-600"
              )}>
                {label}
              </p>
              <p className="text-xs text-gray-500">{description}</p>
            </div>
          </button>
        );
      })}
    </div>
  );
}
