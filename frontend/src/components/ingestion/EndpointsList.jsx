import React, { useState } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Copy, Trash2, Check, ExternalLink, Database } from 'lucide-react';
import { format } from 'date-fns';
import { motion } from 'framer-motion';

export default function EndpointsList() {
  const [copiedId, setCopiedId] = useState(null);
  const queryClient = useQueryClient();

  const { data: endpoints = [], isLoading } = useQuery({
    queryKey: ['ingestionEndpoints'],
    queryFn: () => dataLakeApi.entities.IngestionEndpoint.list('-created_date')
  });

  const deleteMutation = useMutation({
    mutationFn: (id) => dataLakeApi.entities.IngestionEndpoint.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ingestionEndpoints'] });
    }
  });

  const copyEndpoint = async (endpoint) => {
    await navigator.clipboard.writeText(endpoint.endpoint_url);
    setCopiedId(endpoint.id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[#059669]"></div>
      </div>
    );
  }

  if (endpoints.length === 0) {
    return (
      <div className="text-center py-16">
        <Database className="w-12 h-12 text-slate-300 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-slate-700 mb-2">No endpoints created yet</h3>
        <p className="text-slate-500 text-sm">Create your first ingestion endpoint to get started</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {endpoints.map((endpoint) => (
        <motion.div
          key={endpoint.id}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="bg-white border border-slate-200 rounded-xl p-6 hover:shadow-lg transition-all"
        >
          <div className="flex items-start justify-between mb-4">
            <div>
              <div className="flex items-center gap-2 mb-2">
                <Badge variant="outline" className="bg-orange-50 text-orange-600 border-orange-200">
                  {endpoint.domain}
                </Badge>
                <h3 className="text-lg font-semibold text-[#111827]">
                  <code className="text-[#059669]">{endpoint.table_name}</code>
                </h3>
              </div>
              <p className="text-sm text-slate-500 font-mono">
                {endpoint.endpoint_url}
              </p>
            </div>
            <Badge className={endpoint.status === 'active'
              ? "bg-[#D1FAE5] text-[#059669]"
              : "bg-slate-100 text-slate-600"
            }>
              {endpoint.status}
            </Badge>
          </div>

          {/* Schema Preview */}
          {endpoint.schema_definition && endpoint.schema_definition.length > 0 && (
            <div className="mb-4 p-3 bg-slate-50 rounded-lg">
              <p className="text-xs font-medium text-slate-500 mb-2">Schema ({endpoint.schema_mode})</p>
              <div className="flex flex-wrap gap-2">
                {endpoint.schema_definition.slice(0, 5).map((col, idx) => (
                  <Badge key={idx} variant="secondary" className="text-xs font-mono">
                    {col.column_name}: {col.data_type}
                  </Badge>
                ))}
                {endpoint.schema_definition.length > 5 && (
                  <Badge variant="outline" className="text-xs">
                    +{endpoint.schema_definition.length - 5} more
                  </Badge>
                )}
              </div>
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-between items-center pt-3 border-t border-slate-100">
            <Button
              variant="outline"
              size="sm"
              onClick={() => copyEndpoint(endpoint)}
              className="text-[#059669] border-[#059669]/20 hover:bg-[#D1FAE5]"
            >
              {copiedId === endpoint.id ? (
                <>
                  <Check className="w-4 h-4 mr-1" />
                  Copied!
                </>
              ) : (
                <>
                  <Copy className="w-4 h-4 mr-1" />
                  Copy URL
                </>
              )}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => deleteMutation.mutate(endpoint.id)}
              disabled={deleteMutation.isPending}
              className="text-red-600 hover:text-red-700 hover:bg-red-50"
            >
              <Trash2 className="w-4 h-4 mr-1" />
              Delete
            </Button>
          </div>
        </motion.div>
      ))}
    </div>
  );
}
