import React from 'react';
import { useQuery } from '@tanstack/react-query';
import dataLakeApi from '@/api/dataLakeClient';
import { Clock, CheckCircle2, XCircle, ChevronRight } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { Badge } from "@/components/ui/badge";

export default function QueryHistoryPanel({ onSelectQuery }) {
  const { data: history = [] } = useQuery({
    queryKey: ['queryHistory'],
    queryFn: () => dataLakeApi.queryHistory.list(20)
  });

  if (history.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h3 className="font-semibold text-slate-900 mb-4">Query History</h3>
        <div className="text-center py-8">
          <Clock className="w-10 h-10 text-slate-300 mx-auto mb-3" />
          <p className="text-sm text-slate-500">No query history yet</p>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl border border-slate-200">
      <div className="p-4 border-b border-slate-200">
        <h3 className="font-semibold text-slate-900">Query History</h3>
        <p className="text-xs text-slate-500 mt-1">{history.length} recent queries</p>
      </div>

      <div className="max-h-[400px] overflow-y-auto">
        {history.map((item, index) => (
          <button
            key={item.id || index}
            onClick={() => onSelectQuery(item.query)}
            className="w-full px-4 py-3 border-b border-slate-100 hover:bg-slate-50 transition-colors text-left group"
          >
            <div className="flex items-start gap-3">
              {item.status === 'success' ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-500 flex-shrink-0 mt-1" />
              ) : (
                <XCircle className="w-4 h-4 text-red-500 flex-shrink-0 mt-1" />
              )}

              <div className="flex-1 min-w-0">
                <p className="text-xs font-mono text-slate-700 line-clamp-2 group-hover:text-[#059669] transition-colors">
                  {item.query}
                </p>
                <div className="flex items-center gap-3 mt-2">
                  {item.created_date && (
                    <span className="text-xs text-slate-400">
                      {formatDistanceToNow(new Date(item.created_date), { addSuffix: true })}
                    </span>
                  )}
                  {item.rows_returned !== undefined && (
                    <Badge variant="outline" className="text-xs">
                      {item.rows_returned} rows
                    </Badge>
                  )}
                  {item.execution_time_ms && (
                    <span className="text-xs text-slate-400">
                      {item.execution_time_ms}ms
                    </span>
                  )}
                </div>
              </div>

              <ChevronRight className="w-4 h-4 text-slate-300 group-hover:text-slate-500 flex-shrink-0 mt-1" />
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
