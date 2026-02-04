import React from 'react';
import { Wand2, Sparkles } from 'lucide-react';
import { Illustration } from '@/components/ui/sketchy';

export default function AutoInferenceDisplay() {
  return (
    <div
      className="p-6 bg-[#C4B5FD] rounded-2xl border-2 border-[#A78BFA] relative overflow-hidden"
      style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.1)' }}
    >
      <Illustration
        src="/illustrations/magic-wand.png"
        alt="Magic Wand"
        className="absolute right-4 top-1/2 -translate-y-1/2 w-24 h-24 opacity-80"
      />
      <div className="relative z-10">
        <div className="flex items-center gap-2 mb-3">
          <div className="w-10 h-10 bg-[#A78BFA] rounded-xl flex items-center justify-center">
            <Wand2 className="w-5 h-5 text-white" />
          </div>
          <div>
            <h4 className="font-black text-gray-900">Auto Inference</h4>
            <p className="text-xs text-gray-700">Magic schema detection</p>
          </div>
        </div>
        <p className="text-sm text-gray-700 font-medium max-w-sm">
          Schema will be automatically inferred from your first data payload. Just send data!
        </p>
      </div>
    </div>
  );
}
