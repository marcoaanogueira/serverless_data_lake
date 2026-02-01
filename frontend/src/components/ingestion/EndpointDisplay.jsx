import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { CheckCircle2, Copy, Check, ExternalLink } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

export default function EndpointDisplay({ endpoint, tableName }) {
  const [copied, setCopied] = useState(false);

  const copyEndpoint = async () => {
    await navigator.clipboard.writeText(endpoint.endpoint_url);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const curlExample = `curl -X POST "${window.location.origin}${endpoint.endpoint_url}" \\
  -H "Content-Type: application/json" \\
  -d '{"field1": "value1", "field2": "value2"}'`;

  return (
    <div className="space-y-4">
      <div className="p-6 bg-[#D1FAE5] rounded-xl border border-[#059669]/20">
        <div className="flex items-center gap-3 mb-4">
          <CheckCircle2 className="w-6 h-6 text-[#059669]" />
          <h3 className="font-semibold text-[#059669]">Endpoint Created Successfully!</h3>
        </div>

        <div className="space-y-4">
          <div>
            <p className="text-xs font-medium text-[#059669] mb-2">Endpoint URL</p>
            <div className="flex items-center gap-2">
              <code className="flex-1 p-3 bg-white rounded-lg text-sm font-mono text-[#111827] border">
                {endpoint.endpoint_url}
              </code>
              <Button
                variant="outline"
                size="icon"
                onClick={copyEndpoint}
                className="shrink-0"
              >
                {copied ? (
                  <Check className="w-4 h-4 text-[#059669]" />
                ) : (
                  <Copy className="w-4 h-4" />
                )}
              </Button>
            </div>
          </div>

          <div>
            <p className="text-xs font-medium text-[#059669] mb-2">Table Path</p>
            <div className="flex items-center gap-2">
              <Badge variant="outline" className="bg-white">bronze</Badge>
              <span className="text-slate-400">/</span>
              <code className="font-mono text-[#111827]">{tableName}</code>
            </div>
          </div>

          <div>
            <p className="text-xs font-medium text-[#059669] mb-2">Status</p>
            <Badge className="bg-[#059669] text-white">Active</Badge>
          </div>
        </div>
      </div>

      <div className="p-4 bg-slate-900 rounded-lg">
        <p className="text-xs font-medium text-slate-400 mb-2">Example cURL</p>
        <pre className="text-xs font-mono text-slate-100 overflow-x-auto whitespace-pre-wrap">
          {curlExample}
        </pre>
      </div>
    </div>
  );
}
