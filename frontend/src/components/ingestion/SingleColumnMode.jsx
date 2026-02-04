import React from 'react';
import { Braces, Info } from 'lucide-react';

export default function SingleColumnMode() {
  return (
    <div
      className="p-6 bg-[#FECACA] rounded-2xl border-2 border-[#FCA5A5]"
      style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.1)' }}
    >
      <div className="flex items-start gap-4">
        <div className="w-12 h-12 bg-[#FCA5A5] rounded-xl flex items-center justify-center shrink-0">
          <Braces className="w-6 h-6 text-white" />
        </div>
        <div>
          <h4 className="font-black text-gray-900 mb-2">Single Column (JSON)</h4>
          <p className="text-sm text-gray-700 mb-3">
            All data stored in a single <code className="bg-white px-2 py-0.5 rounded-lg font-mono text-sm">data</code> column as JSON.
          </p>
          <div className="flex items-start gap-2 p-2 bg-white rounded-xl">
            <Info className="w-4 h-4 text-gray-400 mt-0.5 shrink-0" />
            <p className="text-xs text-gray-500">
              Query fields with <code className="font-mono">json_extract(data, '$.field')</code>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
