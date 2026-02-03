import React, { useState } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Copy, Trash2, Check, Download, Database, FileCode } from 'lucide-react';
import { motion } from 'framer-motion';
import { cn } from "@/lib/utils";
import { SketchyBadge } from '@/components/ui/sketchy';

const modeColors = {
  manual: { bg: 'bg-[#D4F5E6]', border: 'border-[#A8E6CF]', text: 'text-[#065F46]' },
  auto_inference: { bg: 'bg-[#DDD6FE]', border: 'border-[#C4B5FD]', text: 'text-[#5B21B6]' },
  single_column: { bg: 'bg-[#FEE2E2]', border: 'border-[#FECACA]', text: 'text-[#991B1B]' },
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
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ingestionEndpoints'] })
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
      <div className="flex items-center justify-center py-12">
        <div className="w-10 h-10 rounded-xl bg-[#A8E6CF] animate-pulse" />
      </div>
    );
  }

  if (endpoints.length === 0) {
    return (
      <div className="text-center py-12">
        <Database className="w-12 h-12 text-gray-300 mx-auto mb-4" />
        <h3 className="font-bold text-gray-700 mb-2">No endpoints yet</h3>
        <p className="text-sm text-gray-400">Create your first endpoint to get started</p>
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
            className="bg-white rounded-2xl border-2 border-gray-200 p-5 hover:border-[#A8E6CF] transition-all"
            style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.05)' }}
          >
            {/* Header */}
            <div className="flex items-start justify-between mb-3">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-[#D4F5E6] rounded-xl flex items-center justify-center">
                  <Database className="w-5 h-5 text-[#065F46]" />
                </div>
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <SketchyBadge variant="lilac">{endpoint.domain}</SketchyBadge>
                    <span className="text-gray-300">/</span>
                    <span className="font-black text-gray-900">{endpoint.name}</span>
                    <span className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded-full">
                      v{endpoint.version}
                    </span>
                  </div>
                  <p className="text-xs text-gray-400 font-mono mt-1">{endpoint.endpoint_url}</p>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <span className={cn(
                  "w-2 h-2 rounded-full",
                  endpoint.status === 'active' ? "bg-[#6BCF9F]" : "bg-gray-300"
                )} />
                <span className="text-xs font-bold text-gray-500">{endpoint.status}</span>
              </div>
            </div>

            {/* Mode */}
            <div className={cn(
              "inline-flex items-center gap-2 px-3 py-1.5 rounded-xl text-xs font-bold mb-4",
              modeStyle.bg, modeStyle.border, modeStyle.text, "border"
            )}>
              <FileCode className="w-3.5 h-3.5" />
              {endpoint.mode.replace('_', ' ')}
            </div>

            {/* Actions */}
            <div className="flex justify-between pt-3 border-t border-gray-100">
              <div className="flex gap-2">
                <button
                  onClick={() => copyEndpoint(endpoint)}
                  className={cn(
                    "flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-sm font-bold border-2 transition-all",
                    copiedId === endpoint.id
                      ? "bg-[#6BCF9F] text-white border-[#065F46]"
                      : "bg-white text-[#065F46] border-[#A8E6CF] hover:bg-[#D4F5E6]"
                  )}
                >
                  {copiedId === endpoint.id ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
                  {copiedId === endpoint.id ? 'Copied!' : 'Copy URL'}
                </button>
                <button
                  onClick={() => downloadYaml(endpoint)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-sm font-bold bg-white text-gray-600 border-2 border-gray-200 hover:border-[#C4B5FD] hover:bg-[#DDD6FE] transition-all"
                >
                  <Download className="w-3.5 h-3.5" />
                  YAML
                </button>
              </div>
              <button
                onClick={() => deleteMutation.mutate({ domain: endpoint.domain, name: endpoint.name })}
                disabled={deleteMutation.isPending}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-sm font-bold text-gray-400 hover:text-[#991B1B] hover:bg-[#FEE2E2] transition-all"
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
