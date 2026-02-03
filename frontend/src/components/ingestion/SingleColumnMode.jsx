import React from 'react';
import { FileJson, Info, Braces } from 'lucide-react';

export default function SingleColumnMode() {
  return (
    <div
      className="p-5 bg-[#DDD6FE] rounded-xl border-2 border-[#A78BFA]"
      style={{ boxShadow: '3px 4px 0 rgba(100, 116, 139, 0.1)' }}
    >
      <div className="flex items-start gap-4">
        <div className="w-12 h-12 bg-[#C4B5FD] rounded-xl border-2 border-[#A78BFA] flex items-center justify-center shrink-0">
          <Braces className="w-6 h-6 text-[#7C3AED]" />
        </div>
        <div className="flex-1">
          <h4 className="font-bold text-slate-700 mb-2">Single Column Mode</h4>
          <p className="text-sm text-slate-600 mb-4">
            All incoming data will be stored in a single{' '}
            <code className="bg-white px-2 py-0.5 rounded-lg border-2 border-[#C4B5FD] text-[#7C3AED] font-mono text-xs">
              data
            </code>{' '}
            column as JSON. Perfect for semi-structured or unknown schemas.
          </p>

          {/* Info box */}
          <div className="flex items-start gap-3 p-3 bg-white rounded-xl border-2 border-slate-200">
            <div className="w-6 h-6 rounded-full bg-[#FFF9DB] flex items-center justify-center shrink-0">
              <Info className="w-3.5 h-3.5 text-[#D97706]" />
            </div>
            <p className="text-xs text-slate-500">
              Query individual fields using JSON functions like{' '}
              <code className="bg-slate-100 px-1.5 py-0.5 rounded font-mono text-[#7C3AED]">
                json_extract(data, '$.field')
              </code>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
