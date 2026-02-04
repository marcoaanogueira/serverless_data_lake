import React from 'react';
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Play, Loader2, Clock, CheckCircle2, XCircle, Download } from 'lucide-react';
import { Badge } from "@/components/ui/badge";
import { motion, AnimatePresence } from 'framer-motion';

export default function QueryEditor({ query, onQueryChange, onExecute, isExecuting, results, error, executionTime }) {

  const handleExecute = () => {
    if (query.trim()) {
      onExecute(query);
    }
  };

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  };

  const exportToCSV = () => {
    if (!results || results.length === 0) return;

    const headers = Object.keys(results[0]);
    const csvContent = [
      headers.join(','),
      ...results.map(row =>
        headers.map(header => JSON.stringify(row[header] || '')).join(',')
      )
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    a.click();
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 h-full flex flex-col">
      <div className="p-4 border-b border-gray-200">
        <div className="flex items-center justify-between mb-3">
          <h3 className="font-semibold text-[#111827]">Query Editor</h3>
          <Button
            onClick={handleExecute}
            disabled={isExecuting || !query.trim()}
            size="sm"
            className="bg-[#059669] hover:bg-[#047857] text-white font-semibold"
          >
            {isExecuting ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Executing...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 mr-2" />
                Run Query
              </>
            )}
          </Button>
        </div>
        <Textarea
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="SELECT * FROM bronze.your_table LIMIT 10;"
          className="font-mono text-sm min-h-[120px] resize-none bg-slate-900 text-slate-100 border-slate-700 focus:border-[#059669]"
        />
        <p className="text-xs text-[#6B7280] mt-2">
          Press <kbd className="px-1.5 py-0.5 bg-slate-100 border border-slate-200 rounded text-xs">Ctrl</kbd> + <kbd className="px-1.5 py-0.5 bg-slate-100 border border-slate-200 rounded text-xs">Enter</kbd> to execute
        </p>
      </div>

      <div className="flex-1 overflow-hidden flex flex-col">
        <AnimatePresence mode="wait">
          {results && (
            <motion.div
              key="results"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="flex-1 overflow-auto"
            >
              {/* Status Bar */}
              <div className="px-4 py-3 bg-emerald-50 border-b border-emerald-100 flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                    <span className="text-sm font-medium text-emerald-900">
                      Query executed successfully
                    </span>
                  </div>
                  <Badge variant="outline" className="text-xs bg-white">
                    {results.length} rows
                  </Badge>
                  {executionTime && (
                    <div className="flex items-center gap-1 text-xs text-emerald-700">
                      <Clock className="w-3 h-3" />
                      {executionTime}ms
                    </div>
                  )}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={exportToCSV}
                  className="text-emerald-700 border-emerald-200 hover:bg-emerald-100"
                >
                  <Download className="w-3 h-3 mr-1" />
                  Export CSV
                </Button>
              </div>

              {/* Results Table */}
              {results.length > 0 ? (
                <div className="overflow-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-50 sticky top-0">
                      <tr>
                        {Object.keys(results[0]).map((key) => (
                          <th
                            key={key}
                            className="px-4 py-3 text-left font-medium text-slate-700 border-b border-slate-200 whitespace-nowrap"
                          >
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {results.map((row, idx) => (
                        <tr
                          key={idx}
                          className="border-b border-slate-100 hover:bg-slate-50"
                        >
                          {Object.values(row).map((value, colIdx) => (
                            <td
                              key={colIdx}
                              className="px-4 py-3 text-slate-700 whitespace-nowrap"
                            >
                              {value === null ? (
                                <span className="text-slate-400 italic">null</span>
                              ) : (
                                String(value)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="flex items-center justify-center p-8">
                  <p className="text-slate-500 text-sm">No results returned</p>
                </div>
              )}
            </motion.div>
          )}

          {error && (
            <motion.div
              key="error"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="p-4"
            >
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <div className="flex items-start gap-3">
                  <XCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="font-medium text-red-900 mb-1">Query Error</p>
                    <p className="text-sm text-red-700 font-mono">{error}</p>
                  </div>
                </div>
              </div>
            </motion.div>
          )}

          {!results && !error && !isExecuting && (
            <motion.div
              key="empty"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="flex-1 flex items-center justify-center p-8"
            >
              <div className="text-center">
                <Play className="w-12 h-12 text-slate-300 mx-auto mb-3" />
                <p className="text-slate-500 text-sm">Execute a query to see results</p>
                <p className="text-slate-400 text-xs mt-1">
                  Write your SQL query above and click Run Query
                </p>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
