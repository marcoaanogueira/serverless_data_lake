import React, { useState } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Database, Plus, List, Layers, Search, Sparkles, ArrowRight } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from "@/lib/utils";

import {
  SketchyCard,
  SketchyButton,
  SketchyInput,
  SketchyLabel,
  FloatingElements,
  SketchyFilters,
  SketchyDivider,
  SketchyBadge,
  SketchyTabs,
} from '@/components/ui/sketchy';

import {
  DataPlatformIllustration,
  MagicWandIllustration,
  AnalyticsIllustration,
  PipelineIllustration,
  ServerStackIllustration,
  DatabaseIllustration,
} from '@/components/ui/illustrations';

import SchemaModeTabs from '@/components/ingestion/SchemaModeTabs';
import ManualSchemaForm from '@/components/ingestion/ManualSchemaForm';
import AutoInferenceDisplay from '@/components/ingestion/AutoInferenceDisplay';
import SingleColumnMode from '@/components/ingestion/SingleColumnMode';
import EndpointDisplay from '@/components/ingestion/EndpointDisplay';
import EndpointsList from '@/components/ingestion/EndpointsList';
import GoldJobForm from '@/components/gold/GoldJobForm';
import GoldJobsList from '@/components/gold/GoldJobsList';
import DependencyGraph from '@/components/gold/DependencyGraph';
import TableCatalog from '@/components/query/TableCatalog';
import QueryEditor from '@/components/query/QueryEditor';
import QueryHistoryPanel from '@/components/query/QueryHistoryPanel';

const moduleConfig = {
  ingestion: {
    label: 'Extract',
    icon: Database,
    color: 'mint',
    bgColor: 'bg-[#D4F5E6]',
    borderColor: 'border-[#7DD3B0]',
    textColor: 'text-[#059669]',
    illustration: ServerStackIllustration,
    description: 'Ingest data from any source',
  },
  gold: {
    label: 'Transform',
    icon: Layers,
    color: 'lilac',
    bgColor: 'bg-[#DDD6FE]',
    borderColor: 'border-[#A78BFA]',
    textColor: 'text-[#7C3AED]',
    illustration: PipelineIllustration,
    description: 'Transform with declarative SQL',
  },
  query: {
    label: 'Load',
    icon: Search,
    color: 'coral',
    bgColor: 'bg-[#FFD4D4]',
    borderColor: 'border-[#FF9B9B]',
    textColor: 'text-[#DC2626]',
    illustration: AnalyticsIllustration,
    description: 'Query and analyze your data',
  },
};

export default function DataPlatform() {
  const [activeModule, setActiveModule] = useState('ingestion');
  const [activeTab, setActiveTab] = useState('create');
  const [goldView, setGoldView] = useState('list');

  // Query state
  const [currentQuery, setCurrentQuery] = useState('SELECT * FROM bronze.vendas LIMIT 10;');
  const [queryResults, setQueryResults] = useState(null);
  const [queryError, setQueryError] = useState(null);
  const [isExecutingQuery, setIsExecutingQuery] = useState(false);
  const [executionTime, setExecutionTime] = useState(null);

  // Ingestion state
  const [domain, setDomain] = useState('');
  const [tableName, setTableName] = useState('');
  const [schemaMode, setSchemaMode] = useState('manual');
  const [columns, setColumns] = useState([
    { name: '', type: 'string', required: false, primary_key: false }
  ]);
  const [createdEndpoint, setCreatedEndpoint] = useState(null);
  const [validationError, setValidationError] = useState('');

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
    mutationFn: async (data) => {
      return await dataLakeApi.endpoints.create(data);
    },
    onSuccess: (data) => {
      setCreatedEndpoint(data);
      queryClient.invalidateQueries({ queryKey: ['ingestionEndpoints'] });
    },
    onError: (error) => {
      setValidationError(error.message);
    }
  });

  const createGoldJobMutation = useMutation({
    mutationFn: async (data) => {
      return await dataLakeApi.goldJobs.create(data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['goldJobs'] });
    }
  });

  const executeQuery = async (query) => {
    setIsExecutingQuery(true);
    setQueryError(null);
    setQueryResults(null);

    const startTime = Date.now();

    try {
      const result = await dataLakeApi.executeQuery(query);
      const execTime = Date.now() - startTime;
      setExecutionTime(execTime);
      setQueryResults(result.data || result);

      dataLakeApi.queryHistory.create({
        query: query,
        execution_time_ms: execTime,
        rows_returned: (result.data || result).length,
        status: 'success'
      }).then(() => {
        queryClient.invalidateQueries({ queryKey: ['queryHistory'] });
      }).catch(() => {});
    } catch (error) {
      setQueryError(error.message || 'Query execution failed');
      dataLakeApi.queryHistory.create({
        query: query,
        status: 'error',
        error_message: error.message
      }).catch(() => {});
    } finally {
      setIsExecutingQuery(false);
    }
  };

  const validateAndSubmit = () => {
    setValidationError('');

    const domainValue = domain.trim().toLowerCase().replace(/\s+/g, '_');
    if (!domainValue) {
      setValidationError('Domain is required');
      return;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(domainValue)) {
      setValidationError('Domain must be snake_case (lowercase, start with letter)');
      return;
    }

    const tableValue = tableName.trim().toLowerCase().replace(/\s+/g, '_');
    if (!tableValue) {
      setValidationError('Table name is required');
      return;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(tableValue)) {
      setValidationError('Table name must be snake_case (lowercase, start with letter)');
      return;
    }

    let schemaColumns = [];
    if (schemaMode === 'manual') {
      const validColumns = columns.filter(c => (c.name || c.column_name || '').trim());
      if (validColumns.length === 0) {
        setValidationError('At least one column is required');
        return;
      }

      const names = validColumns.map(c => (c.name || c.column_name).toLowerCase());
      if (new Set(names).size !== names.length) {
        setValidationError('Column names must be unique');
        return;
      }

      schemaColumns = validColumns.map(col => ({
        name: (col.name || col.column_name).toLowerCase().replace(/\s+/g, '_'),
        type: col.type || col.data_type || 'string',
        required: col.required || false,
        primary_key: col.primary_key || col.is_primary_key || false,
      }));
    } else if (schemaMode === 'auto_inference') {
      schemaColumns = [];
    } else if (schemaMode === 'single_column') {
      schemaColumns = [{ name: 'data', type: 'json', required: true, primary_key: false }];
    }

    const backendMode = schemaMode;

    createEndpointMutation.mutate({
      name: tableValue,
      domain: domainValue,
      mode: backendMode,
      columns: schemaColumns,
    });
  };

  const resetIngestionForm = () => {
    setDomain('');
    setTableName('');
    setSchemaMode('manual');
    setColumns([{ name: '', type: 'string', required: false, primary_key: false }]);
    setCreatedEndpoint(null);
    setValidationError('');
  };

  const config = moduleConfig[activeModule];

  return (
    <div className="min-h-screen bg-[#F8FAFC] relative overflow-hidden">
      <SketchyFilters />
      <FloatingElements />

      {/* Navbar - Sketchy Style */}
      <nav className="bg-white/80 backdrop-blur-sm border-b-2 border-slate-200 sticky top-0 z-50">
        <div className="max-w-6xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            {/* Logo */}
            <div className="flex items-center gap-4">
              <div className="relative">
                <div className="w-12 h-12 rounded-xl bg-[#A8E6CF] border-2 border-[#7DD3B0] flex items-center justify-center"
                  style={{ boxShadow: '3px 3px 0 rgba(100, 116, 139, 0.15)' }}>
                  <Database className="w-6 h-6 text-[#059669]" />
                </div>
                <Sparkles className="w-4 h-4 text-[#FBBF24] absolute -top-1 -right-1 animate-sparkle" />
              </div>
              <div>
                <h1 className="text-xl font-bold text-slate-700">Data Platform</h1>
                <p className="text-xs text-slate-400">Serverless Data Lake</p>
              </div>
            </div>

            {/* Module Tabs */}
            <div className="flex gap-2 bg-slate-100 p-1.5 rounded-xl">
              {Object.entries(moduleConfig).map(([key, mod]) => {
                const Icon = mod.icon;
                const isActive = activeModule === key;
                return (
                  <button
                    key={key}
                    onClick={() => setActiveModule(key)}
                    className={cn(
                      "flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all duration-200",
                      isActive
                        ? `${mod.bgColor} ${mod.textColor} border-2 ${mod.borderColor}`
                        : "text-slate-500 hover:text-slate-700 hover:bg-white/50"
                    )}
                    style={isActive ? { boxShadow: '2px 2px 0 rgba(100, 116, 139, 0.1)' } : {}}
                  >
                    <Icon className="w-4 h-4" />
                    <span className="hidden sm:inline">{mod.label}</span>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </nav>

      {/* Content */}
      <div className="max-w-6xl mx-auto px-6 py-8">
        <AnimatePresence mode="wait">
          {/* ===================== INGESTION MODULE ===================== */}
          {activeModule === 'ingestion' && (
            <motion.div
              key="ingestion"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3 }}
            >
              {/* Module Header */}
              <div className="flex items-center gap-6 mb-8">
                <ServerStackIllustration className="w-24 h-28 hidden md:block" />
                <div>
                  <div className="flex items-center gap-3 mb-2">
                    <h2 className="text-3xl font-bold text-slate-700">Extract</h2>
                    <SketchyBadge variant="mint">{endpoints.length} endpoints</SketchyBadge>
                  </div>
                  <p className="text-slate-500 max-w-md">
                    Create ingestion endpoints to receive data from any source. Define your schema or let us infer it automatically.
                  </p>
                </div>
              </div>

              {/* Tabs */}
              <div className="flex gap-3 mb-6">
                <SketchyButton
                  variant={activeTab === 'create' ? 'mint' : 'outline'}
                  onClick={() => {
                    setActiveTab('create');
                    resetIngestionForm();
                  }}
                  className="flex-1 sm:flex-none"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Create New
                </SketchyButton>
                <SketchyButton
                  variant={activeTab === 'list' ? 'mint' : 'outline'}
                  onClick={() => setActiveTab('list')}
                  className="flex-1 sm:flex-none"
                >
                  <List className="w-4 h-4 mr-2" />
                  View All ({endpoints.length})
                </SketchyButton>
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
                    <SketchyCard className="max-w-2xl">
                      <div className="flex items-center gap-3 mb-6">
                        <div className="w-10 h-10 rounded-lg bg-[#D4F5E6] border-2 border-[#7DD3B0] flex items-center justify-center">
                          <Database className="w-5 h-5 text-[#059669]" />
                        </div>
                        <div>
                          <h3 className="text-xl font-bold text-slate-700">New Endpoint</h3>
                          <p className="text-sm text-slate-400">Configure your data ingestion</p>
                        </div>
                      </div>

                      <div className="space-y-6">
                        {/* Domain */}
                        <div className="space-y-2">
                          <SketchyLabel htmlFor="domain">Domain</SketchyLabel>
                          <SketchyInput
                            id="domain"
                            placeholder="sales, ads, finance..."
                            value={domain}
                            onChange={(e) => {
                              setDomain(e.target.value);
                              setValidationError('');
                            }}
                          />
                          <p className="text-xs text-slate-400">
                            Business domain for organizing your data
                          </p>
                        </div>

                        {/* Table Name */}
                        <div className="space-y-2">
                          <SketchyLabel htmlFor="tableName">Table / Dataset Name</SketchyLabel>
                          <SketchyInput
                            id="tableName"
                            placeholder="my_dataset"
                            value={tableName}
                            onChange={(e) => {
                              setTableName(e.target.value);
                              setValidationError('');
                            }}
                            className="font-mono"
                          />
                        </div>

                        <SketchyDivider />

                        {/* Schema Mode */}
                        <div className="space-y-3">
                          <SketchyLabel>Schema Definition Mode</SketchyLabel>
                          <SchemaModeTabs
                            activeMode={schemaMode}
                            onModeChange={(mode) => {
                              setSchemaMode(mode);
                              setValidationError('');
                            }}
                          />
                        </div>

                        {/* Schema Content */}
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
                            {schemaMode === 'auto_inference' && (
                              <div className="flex items-center gap-4 p-4 bg-[#FFF9DB] rounded-xl border-2 border-[#FBBF24]">
                                <MagicWandIllustration className="w-20 h-20" />
                                <div>
                                  <h4 className="font-semibold text-slate-700">Auto Inference Mode</h4>
                                  <p className="text-sm text-slate-500">
                                    Schema will be automatically inferred from your first data payload.
                                    Just send data and we'll figure out the types!
                                  </p>
                                </div>
                              </div>
                            )}
                            {schemaMode === 'single_column' && <SingleColumnMode />}
                          </motion.div>
                        </AnimatePresence>

                        {/* Error */}
                        {validationError && (
                          <div className="flex items-center gap-3 p-4 bg-[#FFD4D4] rounded-xl border-2 border-[#FF9B9B]">
                            <div className="w-8 h-8 rounded-full bg-[#FF9B9B] flex items-center justify-center text-white font-bold">!</div>
                            <p className="text-sm text-[#DC2626]">{validationError}</p>
                          </div>
                        )}

                        {/* Submit */}
                        {!createdEndpoint && (
                          <SketchyButton
                            onClick={validateAndSubmit}
                            disabled={createEndpointMutation.isPending}
                            variant="mint"
                            size="lg"
                            className="w-full"
                          >
                            {createEndpointMutation.isPending ? (
                              <span className="flex items-center gap-2">
                                <svg className="animate-spin w-5 h-5" viewBox="0 0 24 24">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                                </svg>
                                Creating...
                              </span>
                            ) : (
                              <span className="flex items-center gap-2">
                                Create Endpoint
                                <ArrowRight className="w-5 h-5" />
                              </span>
                            )}
                          </SketchyButton>
                        )}

                        {/* Success */}
                        {createdEndpoint && (
                          <div className="space-y-4">
                            <EndpointDisplay endpoint={createdEndpoint} tableName={createdEndpoint.name} />
                            <SketchyButton variant="outline" onClick={resetIngestionForm} className="w-full">
                              Create Another Endpoint
                            </SketchyButton>
                          </div>
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
                      <h3 className="text-xl font-bold text-slate-700 mb-6">All Endpoints</h3>
                      <EndpointsList />
                    </SketchyCard>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          )}

          {/* ===================== TRANSFORM MODULE ===================== */}
          {activeModule === 'gold' && (
            <motion.div
              key="gold"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3 }}
            >
              {/* Module Header */}
              <div className="flex items-center gap-6 mb-8">
                <PipelineIllustration className="w-32 h-28 hidden md:block" />
                <div>
                  <div className="flex items-center gap-3 mb-2">
                    <h2 className="text-3xl font-bold text-slate-700">Transform</h2>
                    <SketchyBadge variant="lilac">{goldJobs.length} jobs</SketchyBadge>
                  </div>
                  <p className="text-slate-500 max-w-md">
                    Create declarative SQL transformations that automatically update when source data changes.
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Create Form */}
                <SketchyCard>
                  <div className="flex items-center gap-3 mb-6">
                    <div className="w-10 h-10 rounded-lg bg-[#DDD6FE] border-2 border-[#A78BFA] flex items-center justify-center">
                      <Plus className="w-5 h-5 text-[#7C3AED]" />
                    </div>
                    <div>
                      <h3 className="text-xl font-bold text-slate-700">New Job</h3>
                      <p className="text-sm text-slate-400">Define a transformation</p>
                    </div>
                  </div>
                  <GoldJobForm
                    existingJobs={goldJobs}
                    onSubmit={(data) => createGoldJobMutation.mutate(data)}
                    isSubmitting={createGoldJobMutation.isPending}
                  />
                </SketchyCard>

                {/* Jobs List / Graph */}
                <div className="space-y-4">
                  <div className="flex gap-2">
                    <SketchyButton
                      variant={goldView === 'list' ? 'lilac' : 'outline'}
                      onClick={() => setGoldView('list')}
                      className="flex-1"
                    >
                      <List className="w-4 h-4 mr-2" />
                      List
                    </SketchyButton>
                    <SketchyButton
                      variant={goldView === 'graph' ? 'lilac' : 'outline'}
                      onClick={() => setGoldView('graph')}
                      className="flex-1"
                    >
                      <Layers className="w-4 h-4 mr-2" />
                      Graph
                    </SketchyButton>
                  </div>

                  <SketchyCard>
                    {goldView === 'list' ? (
                      <>
                        <h3 className="text-xl font-bold text-slate-700 mb-2">Gold Jobs</h3>
                        <p className="text-sm text-slate-400 mb-6">{goldJobs.length} transformations configured</p>
                        <GoldJobsList />
                      </>
                    ) : (
                      <>
                        <h3 className="text-xl font-bold text-slate-700 mb-2">Dependencies</h3>
                        <p className="text-sm text-slate-400 mb-6">Visualize job relationships</p>
                        <DependencyGraph jobs={goldJobs} />
                      </>
                    )}
                  </SketchyCard>
                </div>
              </div>
            </motion.div>
          )}

          {/* ===================== QUERY MODULE ===================== */}
          {activeModule === 'query' && (
            <motion.div
              key="query"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3 }}
            >
              {/* Module Header */}
              <div className="flex items-center gap-6 mb-8">
                <AnalyticsIllustration className="w-28 h-28 hidden md:block" />
                <div>
                  <div className="flex items-center gap-3 mb-2">
                    <h2 className="text-3xl font-bold text-slate-700">Load</h2>
                    <SketchyBadge variant="coral">SQL Editor</SketchyBadge>
                  </div>
                  <p className="text-slate-500 max-w-md">
                    Query your data lake using SQL. Explore tables across bronze, silver, and gold layers.
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-12 gap-6">
                {/* Left: Table Catalog */}
                <div className="col-span-12 lg:col-span-3">
                  <div className="sticky top-24">
                    <SketchyCard className="p-4">
                      <h3 className="font-bold text-slate-700 mb-4 flex items-center gap-2">
                        <Database className="w-4 h-4 text-[#A8E6CF]" />
                        Tables
                      </h3>
                      <TableCatalog />
                    </SketchyCard>
                  </div>
                </div>

                {/* Center: Query Editor */}
                <div className="col-span-12 lg:col-span-6">
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

                {/* Right: Query History */}
                <div className="col-span-12 lg:col-span-3">
                  <div className="sticky top-24">
                    <SketchyCard className="p-4">
                      <h3 className="font-bold text-slate-700 mb-4 flex items-center gap-2">
                        <List className="w-4 h-4 text-[#C4B5FD]" />
                        History
                      </h3>
                      <QueryHistoryPanel
                        onSelectQuery={(query) => {
                          setCurrentQuery(query);
                        }}
                      />
                    </SketchyCard>
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Footer decoration */}
      <div className="fixed bottom-0 left-0 right-0 h-1 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FFB5B5] opacity-50" />
    </div>
  );
}
