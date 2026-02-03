import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Wand2, Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { Badge } from "@/components/ui/badge";
import dataLakeApi from '@/api/dataLakeClient';

export default function AutoInferenceDisplay({ onSchemaInferred }) {
  const [sampleData, setSampleData] = useState('');
  const [inferredSchema, setInferredSchema] = useState(null);
  const [isInferring, setIsInferring] = useState(false);
  const [error, setError] = useState('');

  const inferSchema = async () => {
    if (!sampleData.trim()) return;

    setIsInferring(true);
    setError('');

    try {
      // Parse the JSON first
      const parsed = JSON.parse(sampleData);
      const sample = Array.isArray(parsed) ? parsed[0] : parsed;

      // Call the API to infer schema
      const result = await dataLakeApi.endpoints.infer({ payload: sample });

      // Convert API response to format expected by parent component
      const schema = result.columns.map(col => ({
        name: col.name,
        column_name: col.name,  // For compatibility
        type: col.type,
        data_type: col.type,   // For compatibility
        required: col.required,
        primary_key: col.primary_key,
        is_primary_key: col.primary_key,  // For compatibility
        sample_value: col.sample_value,
      }));

      setInferredSchema(schema);
      onSchemaInferred(schema);
    } catch (e) {
      console.error('Failed to infer schema:', e);
      if (e.message.includes('JSON')) {
        setError('Invalid JSON format. Please check your payload.');
      } else {
        setError(e.message || 'Failed to infer schema');
      }
    } finally {
      setIsInferring(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <p className="text-sm text-slate-600">
          Paste a sample JSON record to automatically infer the schema:
        </p>
        <Textarea
          placeholder={'{\n  "id": 1,\n  "name": "Product A",\n  "price": 99.99,\n  "active": true\n}'}
          value={sampleData}
          onChange={(e) => {
            setSampleData(e.target.value);
            setError('');
          }}
          className="font-mono text-sm min-h-[150px]"
        />
      </div>

      {error && (
        <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 border border-red-100 rounded-lg px-4 py-3">
          <AlertCircle className="w-4 h-4" />
          {error}
        </div>
      )}

      <Button
        type="button"
        onClick={inferSchema}
        disabled={isInferring || !sampleData.trim()}
        className="w-full bg-[#059669] hover:bg-[#047857]"
      >
        {isInferring ? (
          <>
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            Inferring Schema...
          </>
        ) : (
          <>
            <Wand2 className="w-4 h-4 mr-2" />
            Infer Schema
          </>
        )}
      </Button>

      {inferredSchema && (
        <div className="mt-4 p-4 bg-[#D1FAE5] rounded-lg border border-[#059669]/20">
          <div className="flex items-center gap-2 mb-3">
            <CheckCircle2 className="w-5 h-5 text-[#059669]" />
            <span className="font-medium text-[#059669]">Schema Inferred ({inferredSchema.length} columns)</span>
          </div>
          <div className="space-y-2">
            {inferredSchema.map((col, idx) => (
              <div key={idx} className="flex items-center gap-2 text-sm">
                <code className="font-mono text-[#111827]">{col.name}</code>
                <Badge variant="secondary" className="text-xs">
                  {col.type}
                </Badge>
                {col.primary_key && (
                  <Badge className="text-xs bg-amber-100 text-amber-700">PK</Badge>
                )}
                {col.required && (
                  <Badge className="text-xs bg-blue-100 text-blue-700">Required</Badge>
                )}
                {col.sample_value && (
                  <span className="text-xs text-slate-400 truncate max-w-[150px]">
                    ex: {col.sample_value}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
