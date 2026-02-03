import React, { useState } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from "@/components/ui/button";
import { Database, Plus, List, Layers, Search } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from "@/lib/utils";

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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

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
      // Call the consumption API
      const result = await dataLakeApi.executeQuery(query);
      const execTime = Date.now() - startTime;
      setExecutionTime(execTime);
      setQueryResults(result.data || result);

      // Save to history (fire and forget)
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

    // Validate domain (must be snake_case)
    const domainValue = domain.trim().toLowerCase().replace(/\s+/g, '_');
    if (!domainValue) {
      setValidationError('Domain is required');
      return;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(domainValue)) {
      setValidationError('Domain must be snake_case (lowercase, start with letter)');
      return;
    }

    // Validate table name (must be snake_case)
    const tableValue = tableName.trim().toLowerCase().replace(/\s+/g, '_');
    if (!tableValue) {
      setValidationError('Table name is required');
      return;
    }
    if (!/^[a-z][a-z0-9_]*$/.test(tableValue)) {
      setValidationError('Table name must be snake_case (lowercase, start with letter)');
      return;
    }

    // Prepare columns based on schema mode
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

      // Convert to API format
      schemaColumns = validColumns.map(col => ({
        name: (col.name || col.column_name).toLowerCase().replace(/\s+/g, '_'),
        type: col.type || col.data_type || 'string',
        required: col.required || false,
        primary_key: col.primary_key || col.is_primary_key || false,
      }));
    } else if (schemaMode === 'auto_inference') {
      // Auto inference: columns will be inferred from first data payload
      schemaColumns = [];
    } else if (schemaMode === 'single_column') {
      schemaColumns = [{ name: 'data', type: 'json', required: true, primary_key: false }];
    }

    // Use the schema mode as-is (backend supports auto_inference)
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

  return (
    <div className="min-h-screen bg-white">
      {/* Navbar */}
      <nav className="bg-white border-b border-gray-200 sticky top-0 z-50 shadow-sm">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-[#D1FAE5] rounded-lg">
                <Database className="w-6 h-6 text-[#059669]" />
              </div>
              <h1 className="text-xl font-bold text-[#111827]">Data Platform</h1>
            </div>

            <div className="flex gap-2">
              <Button
                variant={activeModule === 'ingestion' ? 'default' : 'ghost'}
                onClick={() => setActiveModule('ingestion')}
                className={cn(
                  activeModule === 'ingestion' && "bg-[#059669] hover:bg-[#047857] text-white"
                )}
              >
                <Database className="w-4 h-4 mr-2" />
                Extract
              </Button>
              <Button
                variant={activeModule === 'gold' ? 'default' : 'ghost'}
                onClick={() => setActiveModule('gold')}
                className={cn(
                  activeModule === 'gold' && "bg-[#059669] hover:bg-[#047857] text-white"
                )}
              >
                <Layers className="w-4 h-4 mr-2" />
                Transform
              </Button>
              <Button
                variant={activeModule === 'query' ? 'default' : 'ghost'}
                onClick={() => setActiveModule('query')}
                className={cn(
                  activeModule === 'query' && "bg-[#059669] hover:bg-[#047857] text-white"
                )}
              >
                <Search className="w-4 h-4 mr-2" />
                Load
              </Button>
            </div>
          </div>
        </div>
      </nav>

      {/* Content */}
      <div className="max-w-5xl mx-auto px-6 py-8">
        <AnimatePresence mode="wait">
          {activeModule === 'ingestion' && (
            <motion.div
              key="ingestion"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              {/* Tabs */}
              <div className="flex gap-2 mb-6">
                <Button
                  variant={activeTab === 'create' ? 'default' : 'outline'}
                  onClick={() => {
                    setActiveTab('create');
                    resetIngestionForm();
                  }}
                  className={cn(
                    "flex-1",
                    activeTab === 'create' && "bg-[#059669] hover:bg-[#047857] text-white"
                  )}
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Create New
                </Button>
                <Button
                  variant={activeTab === 'list' ? 'default' : 'outline'}
                  onClick={() => setActiveTab('list')}
                  className={cn(
                    "flex-1",
                    activeTab === 'list' && "bg-[#059669] hover:bg-[#047857] text-white"
                  )}
                >
                  <List className="w-4 h-4 mr-2" />
                  View Endpoints ({endpoints.length})
                </Button>
              </div>

              {/* Content */}
              {activeTab === 'create' ? (
                <div className="bg-white rounded-3xl shadow-xl border border-gray-200 p-8">
                  <h2 className="text-2xl font-bold text-[#111827] mb-6">Create Ingestion Endpoint</h2>
                  <div className="space-y-8">
                    <div className="space-y-2">
                      <Label htmlFor="domain">Domain</Label>
                      <Input
                        id="domain"
                        placeholder="sales, ads, finance..."
                        value={domain}
                        onChange={(e) => {
                          setDomain(e.target.value);
                          setValidationError('');
                        }}
                        className="h-12"
                      />
                      <p className="text-xs text-slate-500">
                        Business domain for organizing your data (e.g., sales, ads, finance)
                      </p>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="tableName">Table / Dataset Name</Label>
                      <Input
                        id="tableName"
                        placeholder="my_dataset"
                        value={tableName}
                        onChange={(e) => {
                          setTableName(e.target.value);
                          setValidationError('');
                        }}
                        className="font-mono h-12"
                      />
                    </div>

                    <div className="space-y-3">
                      <Label>Schema Definition Mode</Label>
                      <SchemaModeTabs
                        activeMode={schemaMode}
                        onModeChange={(mode) => {
                          setSchemaMode(mode);
                          setValidationError('');
                        }}
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
                        {schemaMode === 'auto_inference' && (
                          <AutoInferenceDisplay />
                        )}
                        {schemaMode === 'single_column' && <SingleColumnMode />}
                      </motion.div>
                    </AnimatePresence>

                    {validationError && (
                      <div className="text-sm text-red-600 bg-red-50 border border-red-100 rounded-lg px-4 py-3">
                        {validationError}
                      </div>
                    )}

                    {!createdEndpoint && (
                      <Button
                        onClick={validateAndSubmit}
                        disabled={createEndpointMutation.isPending}
                        className="w-full h-12 bg-[#059669] hover:bg-[#047857] text-white font-semibold"
                      >
                        {createEndpointMutation.isPending ? 'Creating...' : 'Create Endpoint'}
                      </Button>
                    )}

                    {createdEndpoint && (
                      <>
                        <EndpointDisplay endpoint={createdEndpoint} tableName={createdEndpoint.name} />
                        <Button variant="outline" onClick={resetIngestionForm} className="w-full">
                          Create Another Endpoint
                        </Button>
                      </>
                    )}
                  </div>
                </div>
              ) : (
                <div className="bg-white rounded-3xl shadow-xl border border-gray-200 p-8">
                  <h2 className="text-2xl font-bold text-[#111827] mb-6">All Endpoints</h2>
                  <EndpointsList />
                </div>
              )}
            </motion.div>
          )}

          {activeModule === 'gold' && (
            <motion.div
              key="gold"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Create Form */}
                <div className="bg-white rounded-3xl shadow-xl border border-gray-200 p-8">
                  <h2 className="text-2xl font-bold text-[#111827] mb-2">Create Gold Job</h2>
                  <p className="text-sm text-[#6B7280] mb-6">Define declarative transformations</p>
                  <GoldJobForm
                    existingJobs={goldJobs}
                    onSubmit={(data) => createGoldJobMutation.mutate(data)}
                    isSubmitting={createGoldJobMutation.isPending}
                  />
                </div>

                {/* Jobs List / Graph */}
                <div className="space-y-4">
                  {/* View Toggle */}
                  <div className="flex gap-2">
                    <Button
                      variant={goldView === 'list' ? 'default' : 'outline'}
                      onClick={() => setGoldView('list')}
                      className={cn(
                        "flex-1",
                        goldView === 'list' && "bg-[#059669] hover:bg-[#047857] text-white"
                      )}
                    >
                      <List className="w-4 h-4 mr-2" />
                      List View
                    </Button>
                    <Button
                      variant={goldView === 'graph' ? 'default' : 'outline'}
                      onClick={() => setGoldView('graph')}
                      className={cn(
                        "flex-1",
                        goldView === 'graph' && "bg-[#059669] hover:bg-[#047857] text-white"
                      )}
                    >
                      <Layers className="w-4 h-4 mr-2" />
                      Dependencies
                    </Button>
                  </div>

                  <div className="bg-white rounded-3xl shadow-xl border border-gray-200 p-8">
                    {goldView === 'list' ? (
                      <>
                        <h2 className="text-2xl font-bold text-[#111827] mb-2">Gold Jobs</h2>
                        <p className="text-sm text-[#6B7280] mb-6">{goldJobs.length} jobs configured</p>
                        <GoldJobsList />
                      </>
                    ) : (
                      <>
                        <h2 className="text-2xl font-bold text-[#111827] mb-2">Dependency Graph</h2>
                        <p className="text-sm text-[#6B7280] mb-6">Visualize job dependencies</p>
                        <DependencyGraph jobs={goldJobs} />
                      </>
                    )}
                  </div>
                </div>
              </div>
            </motion.div>
          )}

          {activeModule === 'query' && (
            <motion.div
              key="query"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
            >
              <div className="grid grid-cols-12 gap-6">
                {/* Left: Table Catalog */}
                <div className="col-span-12 lg:col-span-3">
                  <div className="sticky top-24">
                    <TableCatalog />
                  </div>
                </div>

                {/* Center: Query Editor */}
                <div className="col-span-12 lg:col-span-6">
                  <QueryEditor
                    query={currentQuery}
                    onQueryChange={setCurrentQuery}
                    onExecute={executeQuery}
                    isExecuting={isExecutingQuery}
                    results={queryResults}
                    error={queryError}
                    executionTime={executionTime}
                  />
                </div>

                {/* Right: Query History */}
                <div className="col-span-12 lg:col-span-3">
                  <div className="sticky top-24">
                    <QueryHistoryPanel
                      onSelectQuery={(query) => {
                        setCurrentQuery(query);
                      }}
                    />
                  </div>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
