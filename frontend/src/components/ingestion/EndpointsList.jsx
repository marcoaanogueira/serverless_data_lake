import React, { useState } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Copy, Trash2, Check, Download, Database, FileCode, ExternalLink } from 'lucide-react';
import { motion } from 'framer-motion';
import { cn } from "@/lib/utils";
import { SketchyBadge } from '@/components/ui/sketchy';
import { DatabaseIllustration, EmptyStateIllustration } from '@/components/ui/illustrations';

const modeColors = {
  manual: { bg: 'bg-[#D4F5E6]', text: 'text-[#059669]', border: 'border-[#A8E6CF]' },
  auto_inference: { bg: 'bg-[#FFF9DB]', text: 'text-[#D97706]', border: 'border-[#FBBF24]' },
  single_column: { bg: 'bg-[#DDD6FE]', text: 'text-[#7C3AED]', border: 'border-[#C4B5FD]' },
};

export default function EndpointsList() {
  const [copiedId, setCopiedId] = useState(null);
  const queryClient = useQueryClient();

  const { data: endpoints = [], isLoading } = useQuery({
    queryKey: ['ingestionEndpoints'],
    queryFn: () => dataLakeApi.endpoints.list('-updated_at')
  });

  const deleteMutation = useMutation({
    mutationFn: ({ domain, name }) => dataLakeApi.endpoints.delete(domain, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ingestionEndpoints'] });
    }
  });

  const copyEndpoint = async (endpoint) => {
    await navigator.clipboard.writeText(endpoint.endpoint_url);
    setCopiedId(endpoint.id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const downloadYaml = async (endpoint) => {
    try {
      const result = await dataLakeApi.endpoints.getDownloadUrl(endpoint.domain, endpoint.name);
      window.open(result.download_url, '_blank');
    } catch (error) {
      console.error('Failed to download YAML:', error);
    }
  };

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center py-16 gap-4">
        <div className="relative">
          <div className="w-12 h-12 rounded-xl bg-[#A8E6CF] animate-pulse" />
          <div className="absolute inset-0 w-12 h-12 rounded-xl border-2 border-[#7DD3B0] animate-ping opacity-50" />
        </div>
        <p className="text-sm text-slate-400">Loading endpoints...</p>
      </div>
    );
  }

  if (endpoints.length === 0) {
    return (
      <div className="text-center py-12">
        <EmptyStateIllustration className="w-24 h-24 mx-auto mb-4" />
        <h3 className="text-lg font-semibold text-slate-700 mb-2">No endpoints yet</h3>
        <p className="text-slate-400 text-sm max-w-xs mx-auto">
          Create your first ingestion endpoint to start receiving data
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {endpoints.map((endpoint, index) => {
        const modeStyle = modeColors[endpoint.mode] || modeColors.manual;

        return (
          <motion.div
            key={endpoint.id}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.05 }}
            className={cn(
              "bg-white border-2 border-slate-200 rounded-xl p-5",
              "hover:border-[#A8E6CF] transition-all"
            )}
            style={{ boxShadow: '3px 4px 0 rgba(100, 116, 139, 0.08)' }}
          >
            {/* Header */}
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-start gap-3">
                <div className="w-10 h-10 rounded-xl bg-[#D4F5E6] border-2 border-[#A8E6CF] flex items-center justify-center shrink-0">
                  <Database className="w-5 h-5 text-[#059669]" />
                </div>
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <SketchyBadge variant="peach">{endpoint.domain}</SketchyBadge>
                    <span className="text-slate-300">/</span>
                    <h3 className="font-bold text-slate-700">
                      <code className="text-[#059669]">{endpoint.name}</code>
                    </h3>
                    <span className="text-xs text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">
                      v{endpoint.version}
                    </span>
                  </div>
                  <p className="text-xs text-slate-400 font-mono mt-1 flex items-center gap-1">
                    <ExternalLink className="w-3 h-3" />
                    {endpoint.endpoint_url}
                  </p>
                </div>
              </div>

              {/* Status */}
              <div className="flex items-center gap-2">
                <span className={cn(
                  "w-2 h-2 rounded-full",
                  endpoint.status === 'active' ? "bg-[#059669] animate-pulse" : "bg-slate-300"
                )} />
                <span className={cn(
                  "text-xs font-medium",
                  endpoint.status === 'active' ? "text-[#059669]" : "text-slate-400"
                )}>
                  {endpoint.status}
                </span>
              </div>
            </div>

            {/* Mode indicator */}
            <div className={cn(
              "inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm mb-4",
              modeStyle.bg, modeStyle.text, "border", modeStyle.border
            )}>
              <FileCode className="w-3.5 h-3.5" />
              <span className="font-medium">{endpoint.mode.replace('_', ' ')}</span>
            </div>

            {/* Actions */}
            <div className="flex justify-between items-center pt-4 border-t border-slate-100">
              <div className="flex gap-2">
                <button
                  onClick={() => copyEndpoint(endpoint)}
                  className={cn(
                    "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-all",
                    "border-2",
                    copiedId === endpoint.id
                      ? "bg-[#7DD3B0] text-white border-[#059669]"
                      : "bg-white text-[#059669] border-[#A8E6CF] hover:bg-[#D4F5E6]"
                  )}
                >
                  {copiedId === endpoint.id ? (
                    <>
                      <Check className="w-3.5 h-3.5" />
                      Copied!
                    </>
                  ) : (
                    <>
                      <Copy className="w-3.5 h-3.5" />
                      Copy URL
                    </>
                  )}
                </button>
                <button
                  onClick={() => downloadYaml(endpoint)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium bg-white text-slate-600 border-2 border-slate-200 hover:border-[#C4B5FD] hover:bg-[#DDD6FE]/30 transition-all"
                >
                  <Download className="w-3.5 h-3.5" />
                  YAML
                </button>
              </div>
              <button
                onClick={() => deleteMutation.mutate({ domain: endpoint.domain, name: endpoint.name })}
                disabled={deleteMutation.isPending}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium text-[#FF9B9B] hover:bg-[#FFD4D4] hover:text-[#DC2626] transition-all disabled:opacity-50"
              >
                <Trash2 className="w-3.5 h-3.5" />
                Delete
              </button>
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}
