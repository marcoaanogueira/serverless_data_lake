import React from 'react';
import { FileJson, Info } from 'lucide-react';

export default function SingleColumnMode() {
  return (
    <div className="p-6 bg-slate-50 rounded-lg border border-slate-200">
      <div className="flex items-start gap-4">
        <div className="p-3 bg-[#D1FAE5] rounded-lg">
          <FileJson className="w-6 h-6 text-[#059669]" />
        </div>
        <div className="flex-1">
          <h4 className="font-medium text-[#111827] mb-2">Single Column Mode</h4>
          <p className="text-sm text-slate-600 mb-4">
            All incoming data will be stored in a single <code className="bg-white px-1.5 py-0.5 rounded border text-[#059669]">data</code> column as JSON.
            This is useful when you don't know the exact schema upfront or when dealing with semi-structured data.
          </p>
          <div className="flex items-start gap-2 p-3 bg-white rounded border border-slate-200">
            <Info className="w-4 h-4 text-slate-400 mt-0.5 flex-shrink-0" />
            <p className="text-xs text-slate-500">
              You can still query individual fields using JSON functions in your queries,
              like <code className="bg-slate-100 px-1 rounded">json_extract(data, '$.fieldname')</code>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
