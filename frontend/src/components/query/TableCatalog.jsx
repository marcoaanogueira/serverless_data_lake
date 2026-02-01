import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import dataLakeApi from '@/api/dataLakeClient';
import { ChevronRight, Database, Table, Layers, Key } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from "@/lib/utils";

const SchemaSection = ({ schema, tables, icon: Icon, color }) => {
  const [isExpanded, setIsExpanded] = useState(true);

  return (
    <div className="mb-2">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center gap-2 px-3 py-2 hover:bg-slate-100 rounded-lg transition-colors text-left"
      >
        <ChevronRight
          className={cn(
            "w-4 h-4 text-slate-400 transition-transform",
            isExpanded && "rotate-90"
          )}
        />
        <Icon className={cn("w-4 h-4", color)} />
        <span className="font-medium text-sm text-slate-700">{schema}</span>
        <span className="ml-auto text-xs text-slate-400">{tables.length}</span>
      </button>

      <AnimatePresence>
        {isExpanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="ml-6 overflow-hidden"
          >
            {tables.map((table, idx) => (
              <div
                key={idx}
                className="flex items-start gap-2 px-3 py-2 hover:bg-slate-50 rounded-lg cursor-pointer group"
              >
                <Table className="w-3.5 h-3.5 text-slate-400 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-mono text-[#111827] group-hover:text-[#059669] transition-colors">
                    {table.name}
                  </p>
                  {table.columns && table.columns.length > 0 && (
                    <div className="mt-1 space-y-0.5">
                      {table.columns.slice(0, 5).map((col, colIdx) => (
                        <div key={colIdx} className="flex items-center gap-1.5 text-xs">
                          {col.is_primary_key && (
                            <Key className="w-3 h-3 text-amber-500" />
                          )}
                          <span className="text-slate-600">{col.column_name}</span>
                          <span className="text-slate-400">:</span>
                          <span className="text-[#059669]">{col.data_type}</span>
                        </div>
                      ))}
                      {table.columns.length > 5 && (
                        <p className="text-xs text-slate-400 italic">
                          +{table.columns.length - 5} more columns
                        </p>
                      )}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

const DomainSection = ({ domainName, layers }) => {
  const [isDomainExpanded, setIsDomainExpanded] = useState(true);
  const totalTablesInDomain = layers.bronze.length + layers.silver.length + layers.gold.length;

  return (
    <div className="border-b border-slate-100 pb-2 last:border-0">
      <button
        onClick={() => setIsDomainExpanded(!isDomainExpanded)}
        className="w-full flex items-center gap-2 px-3 py-2 hover:bg-slate-100 rounded-lg transition-colors text-left"
      >
        <ChevronRight
          className={cn(
            "w-4 h-4 text-slate-400 transition-transform",
            isDomainExpanded && "rotate-90"
          )}
        />
        <span className="font-semibold text-sm text-slate-700 capitalize">{domainName}</span>
        <span className="ml-auto text-xs text-slate-400">{totalTablesInDomain}</span>
      </button>

      <AnimatePresence>
        {isDomainExpanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="ml-3 overflow-hidden"
          >
            <div className="space-y-1 mt-1">
              {layers.bronze.length > 0 && (
                <SchemaSection
                  schema="bronze"
                  tables={layers.bronze}
                  icon={Database}
                  color="text-orange-600"
                />
              )}

              {layers.silver.length > 0 && (
                <SchemaSection
                  schema="silver"
                  tables={layers.silver}
                  icon={Layers}
                  color="text-slate-500"
                />
              )}

              {layers.gold.length > 0 && (
                <SchemaSection
                  schema="gold"
                  tables={layers.gold}
                  icon={Layers}
                  color="text-amber-500"
                />
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

export default function TableCatalog() {
  const { data: endpoints = [] } = useQuery({
    queryKey: ['ingestionEndpoints'],
    queryFn: () => dataLakeApi.entities.IngestionEndpoint.list()
  });

  const { data: goldJobs = [] } = useQuery({
    queryKey: ['goldJobs'],
    queryFn: () => dataLakeApi.entities.GoldJob.list()
  });

  // Group endpoints and jobs by domain
  const domainGroups = {};

  endpoints.forEach(endpoint => {
    const domain = endpoint.domain || 'uncategorized';
    if (!domainGroups[domain]) {
      domainGroups[domain] = { bronze: [], silver: [], gold: [] };
    }

    domainGroups[domain].bronze.push({
      name: endpoint.table_name,
      columns: endpoint.schema_definition || []
    });

    if (endpoint.schema_definition?.some(col => col.is_primary_key)) {
      domainGroups[domain].silver.push({
        name: endpoint.table_name,
        columns: endpoint.schema_definition || []
      });
    }
  });

  goldJobs.forEach(job => {
    const domain = job.domain || 'uncategorized';
    if (!domainGroups[domain]) {
      domainGroups[domain] = { bronze: [], silver: [], gold: [] };
    }

    domainGroups[domain].gold.push({
      name: job.job_name,
      columns: [
        { column_name: job.partition_column, data_type: 'partition', is_primary_key: false }
      ]
    });
  });

  const totalTables = endpoints.length +
    endpoints.filter(e => e.schema_definition?.some(col => col.is_primary_key)).length +
    goldJobs.length;

  return (
    <div className="bg-white rounded-xl border border-gray-200 h-full flex flex-col">
      <div className="p-4 border-b border-gray-200">
        <div className="flex items-center gap-2 mb-1">
          <Database className="w-5 h-5 text-slate-600" />
          <h3 className="font-semibold text-[#111827]">Data Catalog</h3>
        </div>
        <p className="text-xs text-[#6B7280]">{totalTables} tables across 3 schemas</p>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-2">
        {Object.entries(domainGroups).map(([domainName, layers]) => (
          <DomainSection key={domainName} domainName={domainName} layers={layers} />
        ))}
      </div>

      {totalTables === 0 && (
        <div className="flex-1 flex items-center justify-center p-8">
          <div className="text-center">
            <Database className="w-10 h-10 text-slate-300 mx-auto mb-3" />
            <p className="text-sm text-slate-500">No tables yet</p>
            <p className="text-xs text-slate-400 mt-1">
              Create endpoints or jobs to populate the catalog
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
