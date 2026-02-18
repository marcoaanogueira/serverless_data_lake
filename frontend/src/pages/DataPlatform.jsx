import React, { useState, useRef, useEffect, useCallback } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Database, Plus, List, Layers, Search, Sparkles, ArrowRight, Loader2, Zap, X, Key, Table, Bot, LogOut } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from "@/lib/utils";

import {
  SketchyCard,
  SketchyButton,
  SketchyInput,
  SketchyLabel,
  SketchyBadge,
  SketchyDivider,
  FloatingDecorations,
  Illustration,
  TabButton,
} from '@/components/ui/sketchy';

import SchemaModeTabs from '@/components/ingestion/SchemaModeTabs';
import ManualSchemaForm from '@/components/ingestion/ManualSchemaForm';
import AutoInferenceDisplay from '@/components/ingestion/AutoInferenceDisplay';
import SingleColumnMode from '@/components/ingestion/SingleColumnMode';
import EndpointDisplay from '@/components/ingestion/EndpointDisplay';
import EndpointsList from '@/components/ingestion/EndpointsList';
import GoldJobForm from '@/components/gold/GoldJobForm';
import GoldJobsList from '@/components/gold/GoldJobsList';
import DependencyGraph from '@/components/gold/DependencyGraph';
import OrchestrationOverview from '@/components/gold/OrchestrationOverview';
import TableCatalog from '@/components/query/TableCatalog';
import QueryEditor from '@/components/query/QueryEditor';
import QueryHistoryPanel from '@/components/query/QueryHistoryPanel';
import AiPipeline from '@/components/ai/AiPipeline';

// Illustration paths
const illustrations = {
  serverStack: '/illustrations/server-stack.png',
  pipeline: '/illustrations/pipeline.png',
  analytics: '/illustrations/analytics.png',
  magicWand: '/illustrations/magic-wand.png',
  dataPlatform: '/illustrations/data-platform.png',
};

export default function DataPlatform({ onLogout }) {
  const [activeModule, setActiveModule] = useState('ingestion');
  const [activeTab, setActiveTab] = useState('create');
  const [goldView, setGoldView] = useState('list');

  // Query state
  const [currentQuery, setCurrentQuery] = useState('SELECT * FROM bronze.vendas LIMIT 10;');
  const [queryResults, setQueryResults] = useState(null);
  const [queryError, setQueryError] = useState(null);
  const [isExecutingQuery, setIsExecutingQuery] = useState(false);
  const [executionTime, setExecutionTime] = useState(null);
  const [selectedTable, setSelectedTable] = useState(null);

  // Ingestion state
  const [domain, setDomain] = useState('');
  const [tableName, setTableName] = useState('');
  const [schemaMode, setSchemaMode] = useState('manual');
  const [columns, setColumns] = useState([
    { name: '', type: 'string', required: false, primary_key: false, description: '' }
  ]);
  const [createdEndpoint, setCreatedEndpoint] = useState(null);
  const [validationError, setValidationError] = useState('');

  // Job execution state (lifted here so it persists across tab switches)
  const [runningJobs, setRunningJobs] = useState({});
  const pollIntervals = useRef({});

  const startPolling = useCallback((jobId, executionId) => {
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
  }, []);

  const handleRunJob = useCallback((domain, jobName) => {
    const jobId = `${domain}/${jobName}`;
    setRunningJobs(prev => ({ ...prev, [jobId]: { status: 'RUNNING' } }));
    dataLakeApi.goldJobs.run(domain, jobName)
      .then(data => {
        setRunningJobs(prev => ({
          ...prev,
          [jobId]: { executionId: data.execution_id, status: 'RUNNING', startedAt: data.started_at }
        }));
        startPolling(jobId, data.execution_id);
      })
      .catch(error => {
        setRunningJobs(prev => ({
          ...prev,
          [jobId]: { status: 'FAILED', error: error.message }
        }));
        setTimeout(() => {
          setRunningJobs(prev => {
            const next = { ...prev };
            delete next[jobId];
            return next;
          });
        }, 5000);
      });
  }, [startPolling]);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      Object.values(pollIntervals.current).forEach(clearInterval);
    };
  }, []);

  const queryClient = useQueryClient();

  const { data: endpoints = [] } = useQuery({
    queryKey: ['ingestionEndpoints'],
    queryFn: () => dataLakeApi.endpoints.list()
  });

  const { data: goldJobs = [] } = useQuery({
    queryKey: ['goldJobs'],
    queryFn: () => dataLakeApi.goldJobs.list()
  });

  const createEndpointMutation = useMutation({
    mutationFn: (data) => dataLakeApi.endpoints.create(data),
    onSuccess: (data) => {
      setCreatedEndpoint(data);
      queryClient.invalidateQueries({ queryKey: ['ingestionEndpoints'] });
    },
    onError: (error) => setValidationError(error.message)
  });

  const createGoldJobMutation = useMutation({
    mutationFn: (data) => dataLakeApi.goldJobs.create(data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['goldJobs'] })
  });

  const executeQuery = async (query) => {
    setIsExecutingQuery(true);
    setQueryError(null);
    setQueryResults(null);
    const startTime = Date.now();

    try {
      const result = await dataLakeApi.executeQuery(query);
      setExecutionTime(Date.now() - startTime);
      setQueryResults(result.data || result);
      dataLakeApi.queryHistory.create({
        query, execution_time_ms: Date.now() - startTime,
        rows_returned: (result.data || result).length, status: 'success'
      }).then(() => queryClient.invalidateQueries({ queryKey: ['queryHistory'] })).catch(() => {});
    } catch (error) {
      setQueryError(error.message || 'Query execution failed');
      dataLakeApi.queryHistory.create({
        query, execution_time_ms: Date.now() - startTime,
        rows_returned: 0, status: 'error'
      }).then(() => queryClient.invalidateQueries({ queryKey: ['queryHistory'] })).catch(() => {});
    } finally {
      setIsExecutingQuery(false);
    }
  };

  const validateAndSubmit = () => {
    setValidationError('');
    const domainValue = domain.trim().toLowerCase().replace(/\s+/g, '_');
    const tableValue = tableName.trim().toLowerCase().replace(/\s+/g, '_');

    if (!domainValue) return setValidationError('Domain is required');
    if (!/^[a-z][a-z0-9_]*$/.test(domainValue)) return setValidationError('Domain must be snake_case');
    if (!tableValue) return setValidationError('Table name is required');
    if (!/^[a-z][a-z0-9_]*$/.test(tableValue)) return setValidationError('Table name must be snake_case');

    let schemaColumns = [];
    if (schemaMode === 'manual') {
      const validColumns = columns.filter(c => (c.name || c.column_name || '').trim());
      if (validColumns.length === 0) return setValidationError('At least one column is required');
      schemaColumns = validColumns.map(col => ({
        name: (col.name || col.column_name).toLowerCase().replace(/\s+/g, '_'),
        type: col.type || col.data_type || 'string',
        required: col.required || false,
        primary_key: col.primary_key || col.is_primary_key || false,
        description: col.description || null,
      }));
    } else if (schemaMode === 'single_column') {
      schemaColumns = [{ name: 'data', type: 'json', required: true, primary_key: false }];
    }

    createEndpointMutation.mutate({
      name: tableValue, domain: domainValue, mode: schemaMode, columns: schemaColumns,
    });
  };

  const resetForm = () => {
    setDomain(''); setTableName(''); setSchemaMode('manual');
    setColumns([{ name: '', type: 'string', required: false, primary_key: false, description: '' }]);
    setCreatedEndpoint(null); setValidationError('');
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <FloatingDecorations />

      {/* Light Navbar */}
      <nav className="bg-white border-b-2 border-gray-100 sticky top-0 z-50">
        <div className="max-w-6xl mx-auto px-6 py-3">
          <div className="flex items-center justify-between">
            {/* Logo - Tadpole */}
            <h1 className="text-2xl font-black text-[#1F2937]">
              Tadpole<span className="text-[#FBBF24]">.</span>
            </h1>

            {/* Navigation Tabs + Logout */}
            <div className="flex items-center gap-2">
              {[
                { id: 'ai', label: 'AI Agent', icon: Bot, color: 'dark' },
                { id: 'ingestion', label: 'Extract', icon: Database, color: 'mint' },
                { id: 'gold', label: 'Transform', icon: Layers, color: 'lilac' },
                { id: 'query', label: 'Load', icon: Search, color: 'peach' },
              ].map(({ id, label, icon: Icon, color }) => (
                <button
                  key={id}
                  onClick={() => setActiveModule(id)}
                  className={cn(
                    "flex items-center gap-2 px-4 py-2 rounded-xl font-bold transition-all",
                    activeModule === id
                      ? color === 'mint' ? 'bg-[#A8E6CF] text-[#065F46]'
                        : color === 'lilac' ? 'bg-[#C4B5FD] text-[#5B21B6]'
                        : color === 'dark' ? 'bg-[#1F2937] text-white'
                        : 'bg-[#FECACA] text-[#991B1B]'
                      : "text-gray-500 hover:bg-gray-100"
                  )}
                  style={activeModule === id ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.15)' } : {}}
                >
                  <Icon className="w-5 h-5" />
                  <span className="hidden sm:inline">{label}</span>
                </button>
              ))}

              {onLogout && (
                <button
                  onClick={onLogout}
                  title="Sign out"
                  className="flex items-center gap-1.5 px-3 py-2 rounded-xl text-sm font-medium text-gray-400 hover:text-gray-600 hover:bg-gray-100 transition-all ml-1"
                >
                  <LogOut className="w-4 h-4" />
                  <span className="hidden sm:inline">Sign out</span>
                </button>
              )}
            </div>
          </div>
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto px-6 py-8 relative z-10">

        {/* AI Agent — always mounted so pipeline state persists across tab switches */}
        <div className={activeModule !== 'ai' ? 'hidden' : undefined}>
          <AiPipeline />
        </div>

        <AnimatePresence mode="wait">

          {/* ========== EXTRACT MODULE ========== */}
          {activeModule === 'ingestion' && (
            <motion.div
              key="ingestion"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-6"
            >
              {/* Section Header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <h1 className="text-2xl font-black text-gray-900">Extract</h1>
                  <SketchyBadge variant="mint">{endpoints.length} endpoints</SketchyBadge>
                </div>
              </div>

              {/* Action Tabs */}
              <div className="flex gap-3">
                <TabButton
                  active={activeTab === 'create'}
                  onClick={() => { setActiveTab('create'); resetForm(); }}
                  color="mint"
                >
                  <Plus className="w-4 h-4 inline mr-2" />
                  Create New
                </TabButton>
                <TabButton
                  active={activeTab === 'list'}
                  onClick={() => setActiveTab('list')}
                  color="mint"
                >
                  <List className="w-4 h-4 inline mr-2" />
                  View All ({endpoints.length})
                </TabButton>
              </div>

              {/* Content */}
              <AnimatePresence mode="wait">
                {activeTab === 'create' ? (
                  <motion.div
                    key="create"
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: 20 }}
                  >
                    <SketchyCard>
                      <h2 className="text-2xl font-black text-gray-900 mb-6">
                        New Endpoint
                      </h2>

                      <div className="space-y-5">
                        <div>
                          <SketchyLabel>Domain</SketchyLabel>
                          <SketchyInput
                            placeholder="sales, ads, finance..."
                            value={domain}
                            onChange={(e) => { setDomain(e.target.value); setValidationError(''); }}
                          />
                        </div>

                        <div>
                          <SketchyLabel>Table Name</SketchyLabel>
                          <SketchyInput
                            placeholder="my_dataset"
                            value={tableName}
                            onChange={(e) => { setTableName(e.target.value); setValidationError(''); }}
                            className="font-mono"
                          />
                        </div>

                        <SketchyDivider />

                        <div>
                          <SketchyLabel>Schema Mode</SketchyLabel>
                          <SchemaModeTabs
                            activeMode={schemaMode}
                            onModeChange={(mode) => { setSchemaMode(mode); setValidationError(''); }}
                          />
                        </div>

                        <AnimatePresence mode="wait">
                          <motion.div
                            key={schemaMode}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -10 }}
                          >
                            {schemaMode === 'manual' && (
                              <ManualSchemaForm columns={columns} onColumnsChange={setColumns} />
                            )}
                            {schemaMode === 'auto_inference' && <AutoInferenceDisplay />}
                            {schemaMode === 'single_column' && <SingleColumnMode />}
                          </motion.div>
                        </AnimatePresence>

                        {validationError && (
                          <div className="p-4 bg-[#FEE2E2] rounded-2xl border-2 border-[#FECACA]">
                            <p className="text-[#991B1B] font-bold">{validationError}</p>
                          </div>
                        )}

                        {!createdEndpoint && (
                          <SketchyButton
                            onClick={validateAndSubmit}
                            disabled={createEndpointMutation.isPending}
                            variant="mint"
                            size="lg"
                            className="w-full"
                          >
                            {createEndpointMutation.isPending ? (
                              <><Loader2 className="w-5 h-5 animate-spin inline mr-2" />Creating...</>
                            ) : (
                              <>Create Endpoint <ArrowRight className="w-5 h-5 inline ml-2" /></>
                            )}
                          </SketchyButton>
                        )}

                        {createdEndpoint && (
                          <>
                            <EndpointDisplay endpoint={createdEndpoint} tableName={createdEndpoint.name} />
                            <SketchyButton variant="outline" onClick={resetForm} className="w-full">
                              Create Another
                            </SketchyButton>
                          </>
                        )}
                      </div>
                    </SketchyCard>
                  </motion.div>
                ) : (
                  <motion.div
                    key="list"
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                  >
                    <SketchyCard>
                      <h2 className="text-2xl font-black text-gray-900 mb-6">All Endpoints</h2>
                      <EndpointsList />
                    </SketchyCard>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          )}

          {/* ========== TRANSFORM MODULE ========== */}
          {activeModule === 'gold' && (
            <motion.div
              key="gold"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-6"
            >
              {/* Section Header */}
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-black text-gray-900">Transform</h1>
                <SketchyBadge variant="lilac">{goldJobs.length} jobs</SketchyBadge>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Form */}
                <SketchyCard>
                  <h2 className="text-2xl font-black text-gray-900 mb-6">New Job</h2>
                  <GoldJobForm
                    existingJobs={goldJobs}
                    onSubmit={(data) => createGoldJobMutation.mutate(data)}
                    isSubmitting={createGoldJobMutation.isPending}
                  />
                </SketchyCard>

                {/* List/Graph/Pipelines */}
                <div className="space-y-4">
                  <div className="flex gap-2">
                    <TabButton active={goldView === 'list'} onClick={() => setGoldView('list')} color="lilac">
                      <List className="w-4 h-4 inline mr-2" />List
                    </TabButton>
                    <TabButton active={goldView === 'graph'} onClick={() => setGoldView('graph')} color="lilac">
                      <Layers className="w-4 h-4 inline mr-2" />Graph
                    </TabButton>
                    <TabButton active={goldView === 'pipelines'} onClick={() => setGoldView('pipelines')} color="lilac">
                      <Zap className="w-4 h-4 inline mr-2" />Pipelines
                    </TabButton>
                  </div>

                  <SketchyCard>
                    {goldView === 'list' ? (
                      <>
                        <h2 className="text-xl font-black text-gray-900 mb-4">Gold Jobs</h2>
                        <GoldJobsList runningJobs={runningJobs} onRunJob={handleRunJob} />
                      </>
                    ) : goldView === 'graph' ? (
                      <>
                        <h2 className="text-xl font-black text-gray-900 mb-4">Dependencies</h2>
                        <DependencyGraph jobs={goldJobs} />
                      </>
                    ) : (
                      <>
                        <h2 className="text-xl font-black text-gray-900 mb-4">Scheduled Pipelines</h2>
                        <OrchestrationOverview jobs={goldJobs} />
                      </>
                    )}
                  </SketchyCard>
                </div>
              </div>
            </motion.div>
          )}

          {/* ========== QUERY MODULE ========== */}
          {activeModule === 'query' && (
            <motion.div
              key="query"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-6"
            >
              {/* Section Header */}
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-black text-gray-900">Query</h1>
                <SketchyBadge variant="peach">SQL Editor</SketchyBadge>
              </div>

              <div className="grid grid-cols-12 gap-6">
                <div className="col-span-12 lg:col-span-3">
                  <SketchyCard className="sticky top-24 p-4">
                    <h3 className="font-black text-gray-900 mb-4">Tables</h3>
                    <TableCatalog
                      onSelectTable={(table) => setSelectedTable(prev => prev?.id === table.id ? null : table)}
                      selectedTable={selectedTable}
                    />
                  </SketchyCard>
                </div>

                <div className="col-span-12 lg:col-span-9 space-y-6">
                  <AnimatePresence>
                    {selectedTable && (
                      <motion.div
                        key="table-detail"
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: 'auto', opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.25 }}
                        className="overflow-hidden"
                      >
                        <SketchyCard className="p-5">
                          <div className="flex items-start justify-between mb-4">
                            <div className="flex items-center gap-3">
                              <div className={cn(
                                "w-9 h-9 rounded-lg flex items-center justify-center",
                                selectedTable.schema === 'silver' && "bg-slate-100",
                                selectedTable.schema === 'gold' && "bg-amber-50",
                                selectedTable.schema === 'bronze' && "bg-orange-50",
                              )}>
                                <Layers className={cn(
                                  "w-5 h-5",
                                  selectedTable.schema === 'silver' && "text-slate-500",
                                  selectedTable.schema === 'gold' && "text-amber-500",
                                  selectedTable.schema === 'bronze' && "text-orange-600",
                                )} />
                              </div>
                              <div>
                                <h3 className="font-black text-gray-900">{selectedTable.name}</h3>
                                <p className="text-xs font-mono text-slate-500 mt-0.5">{selectedTable.ref}</p>
                              </div>
                              <SketchyBadge variant={
                                selectedTable.schema === 'gold' ? 'yellow' :
                                selectedTable.schema === 'silver' ? 'default' : 'peach'
                              }>
                                {selectedTable.schema}
                              </SketchyBadge>
                            </div>
                            <button
                              onClick={() => setSelectedTable(null)}
                              className="p-1.5 hover:bg-slate-100 rounded-lg transition-colors text-slate-400 hover:text-slate-600"
                            >
                              <X className="w-4 h-4" />
                            </button>
                          </div>

                          {selectedTable.columns && selectedTable.columns.length > 0 ? (
                            <div className="border border-slate-200 rounded-lg overflow-hidden">
                              <table className="w-full text-sm">
                                <thead>
                                  <tr className="bg-slate-50 border-b border-slate-200">
                                    <th className="text-left px-4 py-2 font-semibold text-slate-600">Column</th>
                                    <th className="text-left px-4 py-2 font-semibold text-slate-600">Type</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {selectedTable.columns.map((col, idx) => (
                                    <tr key={idx} className="border-b border-slate-100 last:border-0">
                                      <td className="px-4 py-2 font-mono text-slate-800 flex items-center gap-2">
                                        {col.primary_key && <Key className="w-3.5 h-3.5 text-amber-500" />}
                                        {col.name}
                                      </td>
                                      <td className="px-4 py-2 font-mono text-emerald-600">{col.type}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : (
                            <div className="text-center py-6 text-slate-400">
                              <Table className="w-8 h-8 mx-auto mb-2 text-slate-300" />
                              <p className="text-sm">No column info available for this table</p>
                            </div>
                          )}
                        </SketchyCard>
                      </motion.div>
                    )}
                  </AnimatePresence>

                  <div className="grid grid-cols-9 gap-6">
                    <div className="col-span-9 lg:col-span-6">
                      <SketchyCard className="p-4">
                        <QueryEditor
                          query={currentQuery}
                          onQueryChange={setCurrentQuery}
                          onExecute={executeQuery}
                          isExecuting={isExecutingQuery}
                          results={queryResults}
                          error={queryError}
                          executionTime={executionTime}
                        />
                      </SketchyCard>
                    </div>

                    <div className="col-span-9 lg:col-span-3">
                      <SketchyCard className="sticky top-24 p-4">
                        <h3 className="font-black text-gray-900 mb-4">History</h3>
                        <QueryHistoryPanel onSelectQuery={setCurrentQuery} />
                      </SketchyCard>
                    </div>
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      {/* Footer gradient */}
      <div className="fixed bottom-0 left-0 right-0 h-1.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA]" />
    </div>
  );
}
