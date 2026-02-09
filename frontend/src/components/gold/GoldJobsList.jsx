import React, { useState, useEffect, useRef } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Copy, Trash2, Check, Database, Clock, GitBranch, Code, RefreshCw, Plus, Key, Play, Loader2, CheckCircle2, XCircle, AlertCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

export default function GoldJobsList() {
  const [copiedId, setCopiedId] = useState(null);
  const [selectedDomain, setSelectedDomain] = useState('all');
  const [runningJobs, setRunningJobs] = useState({});  // { "domain/job_name": { executionId, status } }
  const pollIntervals = useRef({});
  const queryClient = useQueryClient();

  const { data: jobs = [], isLoading } = useQuery({
    queryKey: ['goldJobs'],
    queryFn: () => dataLakeApi.goldJobs.list('-created_date')
  });

  const deleteMutation = useMutation({
    mutationFn: (id) => dataLakeApi.goldJobs.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['goldJobs'] });
    }
  });

  const runMutation = useMutation({
    mutationFn: ({ domain, jobName }) => dataLakeApi.goldJobs.run(domain, jobName),
    onSuccess: (data, { domain, jobName }) => {
      const jobId = `${domain}/${jobName}`;
      setRunningJobs(prev => ({
        ...prev,
        [jobId]: { executionId: data.execution_id, status: 'RUNNING', startedAt: data.started_at }
      }));
      startPolling(jobId, data.execution_id);
    },
    onError: (error, { domain, jobName }) => {
      const jobId = `${domain}/${jobName}`;
      setRunningJobs(prev => ({
        ...prev,
        [jobId]: { status: 'FAILED', error: error.message }
      }));
      // Clear error after 5 seconds
      setTimeout(() => {
        setRunningJobs(prev => {
          const next = { ...prev };
          delete next[jobId];
          return next;
        });
      }, 5000);
    }
  });

  const startPolling = (jobId, executionId) => {
    // Clear any existing interval for this job
    if (pollIntervals.current[jobId]) {
      clearInterval(pollIntervals.current[jobId]);
    }
    pollIntervals.current[jobId] = setInterval(async () => {
      try {
        const result = await dataLakeApi.goldJobs.getExecution(executionId);
        if (result.status !== 'RUNNING') {
          clearInterval(pollIntervals.current[jobId]);
          delete pollIntervals.current[jobId];
          setRunningJobs(prev => ({
            ...prev,
            [jobId]: { ...prev[jobId], status: result.status, stoppedAt: result.stopped_at }
          }));
          // Clear terminal status after 8 seconds
          setTimeout(() => {
            setRunningJobs(prev => {
              const next = { ...prev };
              delete next[jobId];
              return next;
            });
          }, 8000);
        }
      } catch {
        // Keep polling on transient errors
      }
    }, 5000);
  };

  // Cleanup intervals on unmount
  useEffect(() => {
    return () => {
      Object.values(pollIntervals.current).forEach(clearInterval);
    };
  }, []);

  const generateYAML = (job) => {
    const yamlLines = [`- job_name: ${job.job_name || job.name}`];
    yamlLines.push(`  query: |`);
    (job.query || '').split('\n').forEach(line => {
      yamlLines.push(`    ${line}`);
    });
    yamlLines.push(`  write_mode: ${job.write_mode || 'overwrite'}`);
    if (job.unique_key) {
      yamlLines.push(`  unique_key: ${job.unique_key}`);
    }

    if (job.schedule_type === 'cron') {
      yamlLines.push(`  cron: "${job.cron_schedule}"`);
    } else if (job.dependencies?.length > 0) {
      yamlLines.push(`  dependencies:`);
      job.dependencies.forEach(dep => {
        yamlLines.push(`    - ${dep}`);
      });
    }

    return yamlLines.join('\n');
  };

  const copyYAML = async (job) => {
    const yaml = generateYAML(job);
    await navigator.clipboard.writeText(yaml);
    setCopiedId(job.id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[#059669]"></div>
      </div>
    );
  }

  // Get unique domains
  const domains = [...new Set(jobs.map(job => job.domain || 'uncategorized'))];

  // Filter jobs by selected domain
  const filteredJobs = selectedDomain === 'all'
    ? jobs
    : jobs.filter(job => (job.domain || 'uncategorized') === selectedDomain);

  if (jobs.length === 0) {
    return (
      <div className="text-center py-16">
        <Database className="w-12 h-12 text-slate-300 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-slate-700 mb-2">No gold jobs created yet</h3>
        <p className="text-slate-500 text-sm">Create your first declarative job to get started</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Domain Filter */}
      <div className="flex items-center gap-3 bg-white p-4 rounded-lg border border-gray-200">
        <span className="text-sm font-medium text-[#111827]">Domain:</span>
        <Select value={selectedDomain} onValueChange={setSelectedDomain}>
          <SelectTrigger className="w-48">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Domains ({jobs.length})</SelectItem>
            {domains.map(domain => (
              <SelectItem key={domain} value={domain} className="capitalize">
                {domain} ({jobs.filter(j => (j.domain || 'uncategorized') === domain).length})
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Jobs List */}
      <div className="space-y-4">
        {filteredJobs.map((job, index) => (
          <motion.div
            key={job.id || index}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-white border border-gray-200 rounded-xl p-6 hover:shadow-lg transition-all"
          >
            <div className="flex items-start justify-between mb-4">
              <div>
                <h3 className="text-lg font-semibold text-[#111827] mb-1">
                  <code className="text-[#059669]">gold.{job.job_name || job.name}</code>
                </h3>
                <div className="flex items-center gap-3 mt-2">
                  {job.schedule_type === 'cron' ? (
                    <Badge variant="outline" className="text-xs flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      {job.cron_schedule}ly
                    </Badge>
                  ) : (
                    <Badge variant="outline" className="text-xs flex items-center gap-1 bg-purple-50 text-purple-700 border-purple-200">
                      <GitBranch className="w-3 h-3" />
                      Dependency-based
                    </Badge>
                  )}
                  {/* Execution status indicator */}
                  <AnimatePresence>
                    {runningJobs[job.id] && (
                      <motion.div
                        initial={{ opacity: 0, scale: 0.8 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 0.8 }}
                      >
                        {runningJobs[job.id].status === 'RUNNING' && (
                          <Badge className="text-xs flex items-center gap-1 bg-blue-50 text-blue-700 border-blue-200">
                            <Loader2 className="w-3 h-3 animate-spin" />
                            Running
                          </Badge>
                        )}
                        {runningJobs[job.id].status === 'SUCCEEDED' && (
                          <Badge className="text-xs flex items-center gap-1 bg-green-50 text-green-700 border-green-200">
                            <CheckCircle2 className="w-3 h-3" />
                            Succeeded
                          </Badge>
                        )}
                        {runningJobs[job.id].status === 'FAILED' && (
                          <Badge className="text-xs flex items-center gap-1 bg-red-50 text-red-700 border-red-200">
                            <XCircle className="w-3 h-3" />
                            Failed
                          </Badge>
                        )}
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              </div>
            </div>

            {/* Query */}
            {job.query && (
              <div className="mb-4">
                <p className="text-xs font-medium text-[#6B7280] mb-2 flex items-center gap-1">
                  <Code className="w-3 h-3" />
                  Query:
                </p>
                <div className="bg-[#D1FAE5]/30 text-[#059669] rounded-lg p-3 overflow-x-auto border border-gray-200">
                  <pre className="text-xs font-mono">{job.query}</pre>
                </div>
              </div>
            )}

            {/* Write Mode + Unique Key */}
            <div className="mb-4 flex items-center gap-2 flex-wrap">
              {job.write_mode === 'overwrite' ? (
                <Badge variant="outline" className="text-xs flex items-center gap-1 bg-orange-50 text-orange-700 border-orange-200">
                  <RefreshCw className="w-3 h-3" />
                  Overwrite
                </Badge>
              ) : (
                <Badge variant="outline" className="text-xs flex items-center gap-1 bg-[#D1FAE5] text-[#059669] border-[#059669]/20">
                  <Plus className="w-3 h-3" />
                  {job.unique_key ? 'Upsert' : 'Append'}
                </Badge>
              )}
              {job.unique_key && (
                <span className="text-xs text-[#6B7280] flex items-center gap-1">
                  <Key className="w-3 h-3" />
                  Key: <code className="font-mono text-[#059669] bg-[#D1FAE5]/30 px-1.5 py-0.5 rounded border border-gray-200">{job.unique_key}</code>
                </span>
              )}
            </div>

            {/* Dependencies */}
            {job.schedule_type === 'dependency' && job.dependencies?.length > 0 && (
              <div className="mb-4">
                <p className="text-xs font-medium text-[#6B7280] mb-2">Dependencies:</p>
                <div className="flex flex-wrap gap-2">
                  {job.dependencies.map((dep, idx) => (
                    <Badge key={idx} variant="secondary" className="text-xs font-mono bg-[#D1FAE5] text-[#059669] border border-[#059669]/20">
                      {dep}
                    </Badge>
                  ))}
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex justify-between items-center pt-3 border-t border-gray-200">
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    const [domain, jobName] = (job.id || '').split('/');
                    if (domain && jobName) runMutation.mutate({ domain, jobName });
                  }}
                  disabled={runningJobs[job.id]?.status === 'RUNNING' || runMutation.isPending}
                  className="text-blue-600 border-blue-200 hover:bg-blue-50"
                >
                  {runningJobs[job.id]?.status === 'RUNNING' ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-1 animate-spin" />
                      Running...
                    </>
                  ) : (
                    <>
                      <Play className="w-4 h-4 mr-1" />
                      Run
                    </>
                  )}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => copyYAML(job)}
                  className="text-[#059669] border-[#059669]/20 hover:bg-[#D1FAE5]"
                >
                  {copiedId === job.id ? (
                    <>
                      <Check className="w-4 h-4 mr-1" />
                      Copied!
                    </>
                  ) : (
                    <>
                      <Copy className="w-4 h-4 mr-1" />
                      YAML
                    </>
                  )}
                </Button>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => deleteMutation.mutate(job.id)}
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
    </div>
  );
}
