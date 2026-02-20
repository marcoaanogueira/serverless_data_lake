import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Play, Trash2, Loader2, ChevronDown, ChevronUp,
  Clock, CheckCircle2, XCircle,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import dataLakeApi from '@/api/dataLakeClient';
import { SketchyButton, SketchyBadge } from '@/components/ui/sketchy';

function PlanCard({ plan, onRunNow, onDelete, runState }) {
  const [expanded, setExpanded] = useState(false);
  const endpoints = plan.plan?.endpoints || [];
  const tags = plan.tags || [];

  return (
    <div
      className={cn(
        'border-2 rounded-2xl overflow-hidden bg-white transition-colors',
        expanded ? 'border-[#A8E6CF]' : 'border-gray-200 hover:border-[#A8E6CF]',
      )}
    >
      {/* Header row — click to expand */}
      <button
        className="w-full flex items-center gap-3 px-4 py-3 text-left"
        onClick={() => setExpanded(v => !v)}
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-0.5">
            <span className="font-black text-gray-900 font-mono text-sm">{plan.plan_name}</span>
            <SketchyBadge variant="mint">{plan.domain}</SketchyBadge>
            {tags.map(t => (
              <span
                key={t}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold bg-gray-100 text-gray-600 border border-gray-200"
              >
                <Clock className="w-3 h-3" />{t}
              </span>
            ))}
          </div>
          <p className="text-xs text-gray-400 truncate">
            {plan.plan?.api_name ? `${plan.plan.api_name} — ` : ''}
            {endpoints.length} endpoint{endpoints.length !== 1 ? 's' : ''}
          </p>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {runState?.status === 'running' && <Loader2 className="w-4 h-4 animate-spin text-emerald-600" />}
          {runState?.status === 'success' && <CheckCircle2 className="w-4 h-4 text-emerald-600" />}
          {runState?.status === 'error' && <XCircle className="w-4 h-4 text-red-500" />}
          {expanded
            ? <ChevronUp className="w-4 h-4 text-gray-400" />
            : <ChevronDown className="w-4 h-4 text-gray-400" />}
        </div>
      </button>

      {/* Expandable body */}
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            key="body"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-4 pb-4 space-y-3 border-t border-gray-100">
              {/* Endpoints table */}
              {endpoints.length > 0 && (
                <div className="mt-3 border border-gray-100 rounded-xl overflow-hidden">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-gray-50">
                        <th className="text-left px-3 py-2 font-semibold text-gray-500">Resource</th>
                        <th className="text-left px-3 py-2 font-semibold text-gray-500 hidden sm:table-cell">Description</th>
                      </tr>
                    </thead>
                    <tbody>
                      {endpoints.map((ep, i) => (
                        <tr key={i} className="border-t border-gray-100">
                          <td className="px-3 py-2 font-mono text-gray-800">{ep.resource_name}</td>
                          <td className="px-3 py-2 text-gray-500 hidden sm:table-cell">{ep.description || '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-2">
                <SketchyButton
                  size="sm"
                  variant="mint"
                  onClick={(e) => { e.stopPropagation(); onRunNow(plan.plan_name); }}
                  disabled={runState?.status === 'running'}
                >
                  {runState?.status === 'running'
                    ? <><Loader2 className="w-3.5 h-3.5 animate-spin inline mr-1" />Running...</>
                    : <><Play className="w-3.5 h-3.5 inline mr-1" />Run Now</>}
                </SketchyButton>
                <SketchyButton
                  size="sm"
                  variant="peach"
                  onClick={(e) => { e.stopPropagation(); onDelete(plan.plan_name); }}
                  disabled={runState?.status === 'running'}
                >
                  <Trash2 className="w-3.5 h-3.5 inline mr-1" />Delete
                </SketchyButton>
              </div>

              {runState?.error && (
                <p className="text-xs text-red-600 font-medium">{runState.error}</p>
              )}
              {runState?.status === 'success' && (
                <p className="text-xs text-emerald-600 font-medium">Execution started successfully.</p>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default function IngestionPlansList() {
  const queryClient = useQueryClient();
  const [runStates, setRunStates] = useState({});

  const { data: plans = [], isLoading } = useQuery({
    queryKey: ['ingestionPlans'],
    queryFn: () => dataLakeApi.ingestionPlans.list(),
  });

  const deleteMutation = useMutation({
    mutationFn: (planName) => dataLakeApi.ingestionPlans.delete(planName),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ingestionPlans'] }),
  });

  const handleRunNow = async (planName) => {
    setRunStates(prev => ({ ...prev, [planName]: { status: 'running' } }));
    try {
      await dataLakeApi.ingestionPlans.run(planName);
      setRunStates(prev => ({ ...prev, [planName]: { status: 'success' } }));
      setTimeout(() => {
        setRunStates(prev => {
          const next = { ...prev };
          delete next[planName];
          return next;
        });
      }, 5000);
    } catch (err) {
      setRunStates(prev => ({ ...prev, [planName]: { status: 'error', error: err.message } }));
    }
  };

  const handleDelete = (planName) => {
    if (!confirm(`Delete plan "${planName}"? This cannot be undone.`)) return;
    deleteMutation.mutate(planName);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  if (plans.length === 0) {
    return (
      <div className="text-center py-12 text-gray-400">
        <p className="text-sm font-bold">No ingestion plans yet.</p>
        <p className="text-xs mt-1">Run the AI Agent to generate your first plan.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {plans.map(plan => (
        <PlanCard
          key={plan.plan_name}
          plan={plan}
          onRunNow={handleRunNow}
          onDelete={handleDelete}
          runState={runStates[plan.plan_name]}
        />
      ))}
    </div>
  );
}
