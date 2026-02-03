import React from 'react';
import { Wand2, Sparkles } from 'lucide-react';

export default function AutoInferenceDisplay() {
  return (
    <div className="p-6 bg-gradient-to-br from-[#D1FAE5] to-[#A7F3D0] rounded-xl border border-[#059669]/20">
      <div className="flex items-start gap-4">
        <div className="p-3 bg-white rounded-lg shadow-sm">
          <Wand2 className="w-6 h-6 text-[#059669]" />
        </div>
        <div className="flex-1">
          <h3 className="font-semibold text-[#065F46] flex items-center gap-2">
            Auto Inference Mode
            <Sparkles className="w-4 h-4 text-amber-500" />
          </h3>
          <p className="text-sm text-[#047857] mt-1">
            The schema will be automatically inferred from the first data payload received.
          </p>
          <ul className="mt-3 text-sm text-[#065F46] space-y-1">
            <li>• Column names and types detected automatically</li>
            <li>• Schema versioned on each structure change</li>
            <li>• No manual configuration required</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
