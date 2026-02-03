import React, { useState } from 'react';
import { CheckCircle2, Copy, Check, Sparkles, Terminal, Database } from 'lucide-react';
import { cn } from "@/lib/utils";
import { SketchyBadge } from '@/components/ui/sketchy';

export default function EndpointDisplay({ endpoint, tableName }) {
  const [copied, setCopied] = useState(false);
  const [copiedCurl, setCopiedCurl] = useState(false);

  const copyEndpoint = async () => {
    await navigator.clipboard.writeText(endpoint.endpoint_url);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const curlExample = `curl -X POST "${window.location.origin}${endpoint.endpoint_url}" \\
  -H "Content-Type: application/json" \\
  -d '{"field1": "value1", "field2": "value2"}'`;

  const copyCurl = async () => {
    await navigator.clipboard.writeText(curlExample);
    setCopiedCurl(true);
    setTimeout(() => setCopiedCurl(false), 2000);
  };

  return (
    <div className="space-y-4">
      {/* Success Card */}
      <div
        className="relative p-6 bg-[#D4F5E6] rounded-xl border-2 border-[#7DD3B0]"
        style={{ boxShadow: '4px 5px 0 rgba(100, 116, 139, 0.1)' }}
      >
        {/* Sparkle decoration */}
        <Sparkles className="absolute -top-2 -right-2 w-6 h-6 text-[#FBBF24] animate-sparkle" />

        {/* Header */}
        <div className="flex items-center gap-3 mb-5">
          <div className="w-10 h-10 rounded-xl bg-[#7DD3B0] flex items-center justify-center">
            <CheckCircle2 className="w-5 h-5 text-white" />
          </div>
          <div>
            <h3 className="font-bold text-[#059669]">Endpoint Created!</h3>
            <p className="text-xs text-[#059669]/70">Ready to receive data</p>
          </div>
        </div>

        <div className="space-y-4">
          {/* Endpoint URL */}
          <div>
            <p className="text-xs font-semibold text-[#059669] mb-2 flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-[#059669]" />
              Endpoint URL
            </p>
            <div className="flex items-center gap-2">
              <code className="flex-1 p-3 bg-white rounded-xl text-sm font-mono text-slate-700 border-2 border-[#A8E6CF]">
                {endpoint.endpoint_url}
              </code>
              <button
                onClick={copyEndpoint}
                className={cn(
                  "p-3 rounded-xl border-2 transition-all",
                  copied
                    ? "bg-[#7DD3B0] border-[#059669] text-white"
                    : "bg-white border-[#A8E6CF] text-[#059669] hover:bg-[#D4F5E6]"
                )}
              >
                {copied ? (
                  <Check className="w-4 h-4" />
                ) : (
                  <Copy className="w-4 h-4" />
                )}
              </button>
            </div>
          </div>

          {/* Table Path */}
          <div>
            <p className="text-xs font-semibold text-[#059669] mb-2 flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-[#059669]" />
              Table Path
            </p>
            <div className="flex items-center gap-2 p-3 bg-white rounded-xl border-2 border-[#A8E6CF]">
              <Database className="w-4 h-4 text-[#A8E6CF]" />
              <SketchyBadge variant="mint">bronze</SketchyBadge>
              <span className="text-slate-300">/</span>
              <code className="font-mono text-slate-700">{tableName}</code>
            </div>
          </div>

          {/* Status */}
          <div className="flex items-center gap-3">
            <p className="text-xs font-semibold text-[#059669]">Status:</p>
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-[#059669] animate-pulse" />
              <span className="text-sm font-medium text-[#059669]">Active</span>
            </div>
          </div>
        </div>
      </div>

      {/* cURL Example */}
      <div
        className="relative p-4 bg-slate-800 rounded-xl border-2 border-slate-700"
        style={{ boxShadow: '3px 4px 0 rgba(0, 0, 0, 0.2)' }}
      >
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Terminal className="w-4 h-4 text-[#A8E6CF]" />
            <p className="text-xs font-medium text-slate-400">Example cURL</p>
          </div>
          <button
            onClick={copyCurl}
            className={cn(
              "px-2 py-1 rounded-lg text-xs font-medium transition-all flex items-center gap-1",
              copiedCurl
                ? "bg-[#7DD3B0] text-white"
                : "bg-slate-700 text-slate-300 hover:bg-slate-600"
            )}
          >
            {copiedCurl ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
            {copiedCurl ? 'Copied!' : 'Copy'}
          </button>
        </div>
        <pre className="text-xs font-mono text-[#A8E6CF] overflow-x-auto whitespace-pre-wrap">
          {curlExample}
        </pre>
      </div>
    </div>
  );
}
