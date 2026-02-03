import React, { useState } from 'react';
import { CheckCircle2, Copy, Check, Terminal, Database } from 'lucide-react';
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
  -d '{"field1": "value1"}'`;

  const copyCurl = async () => {
    await navigator.clipboard.writeText(curlExample);
    setCopiedCurl(true);
    setTimeout(() => setCopiedCurl(false), 2000);
  };

  return (
    <div className="space-y-4">
      {/* Success Card */}
      <div
        className="p-6 bg-[#A8E6CF] rounded-2xl border-2 border-[#6BCF9F]"
        style={{ boxShadow: '4px 5px 0 rgba(0,0,0,0.1)' }}
      >
        <div className="flex items-center gap-3 mb-4">
          <div className="w-10 h-10 bg-[#6BCF9F] rounded-xl flex items-center justify-center">
            <CheckCircle2 className="w-5 h-5 text-white" />
          </div>
          <div>
            <h3 className="font-black text-gray-900">Endpoint Created!</h3>
            <p className="text-xs text-gray-700">Ready to receive data</p>
          </div>
        </div>

        {/* URL */}
        <div className="mb-4">
          <p className="text-xs font-bold text-gray-700 mb-2">Endpoint URL</p>
          <div className="flex gap-2">
            <code className="flex-1 p-3 bg-white rounded-xl text-sm font-mono border-2 border-[#6BCF9F] truncate">
              {endpoint.endpoint_url}
            </code>
            <button
              onClick={copyEndpoint}
              className={cn(
                "px-4 rounded-xl border-2 font-bold transition-all",
                copied
                  ? "bg-[#6BCF9F] border-[#065F46] text-white"
                  : "bg-white border-[#6BCF9F] text-[#065F46] hover:bg-[#D4F5E6]"
              )}
            >
              {copied ? <Check className="w-4 h-4" /> : <Copy className="w-4 h-4" />}
            </button>
          </div>
        </div>

        {/* Table */}
        <div className="flex items-center gap-2">
          <Database className="w-4 h-4 text-[#065F46]" />
          <SketchyBadge variant="mint">bronze</SketchyBadge>
          <span className="text-gray-500">/</span>
          <code className="font-mono font-bold text-gray-900">{tableName}</code>
        </div>
      </div>

      {/* cURL */}
      <div
        className="p-4 bg-[#1F2937] rounded-2xl border-2 border-[#374151]"
        style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.2)' }}
      >
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Terminal className="w-4 h-4 text-[#A8E6CF]" />
            <p className="text-xs font-bold text-gray-400">cURL Example</p>
          </div>
          <button
            onClick={copyCurl}
            className={cn(
              "px-3 py-1 rounded-lg text-xs font-bold transition-all",
              copiedCurl
                ? "bg-[#6BCF9F] text-white"
                : "bg-[#374151] text-gray-300 hover:bg-[#4B5563]"
            )}
          >
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
