import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Wand2, Loader2, CheckCircle2 } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

export default function AutoInferenceDisplay({ onSchemaInferred }) {
  const [sampleData, setSampleData] = useState('');
  const [inferredSchema, setInferredSchema] = useState(null);
  const [isInferring, setIsInferring] = useState(false);

  const inferSchema = async () => {
    if (!sampleData.trim()) return;

    setIsInferring(true);

    // Simulate schema inference
    setTimeout(() => {
      try {
        const parsed = JSON.parse(sampleData);
        const sample = Array.isArray(parsed) ? parsed[0] : parsed;

        const schema = Object.entries(sample).map(([key, value]) => {
          let dataType = 'varchar';
          if (typeof value === 'number') {
            dataType = Number.isInteger(value) ? 'integer' : 'float';
          } else if (typeof value === 'boolean') {
            dataType = 'boolean';
          } else if (value instanceof Date || (typeof value === 'string' && !isNaN(Date.parse(value)))) {
            dataType = 'timestamp';
          } else if (typeof value === 'object') {
            dataType = 'json';
          }

          return {
            column_name: key,
            data_type: dataType,
            required: false,
            is_primary_key: key === 'id'
          };
        });

        setInferredSchema(schema);
        onSchemaInferred(schema);
      } catch (e) {
        console.error('Failed to parse JSON:', e);
      } finally {
        setIsInferring(false);
      }
    }, 1000);
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
          onChange={(e) => setSampleData(e.target.value)}
          className="font-mono text-sm min-h-[150px]"
        />
      </div>

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
            <span className="font-medium text-[#059669]">Schema Inferred</span>
          </div>
          <div className="space-y-2">
            {inferredSchema.map((col, idx) => (
              <div key={idx} className="flex items-center gap-2 text-sm">
                <code className="font-mono text-[#111827]">{col.column_name}</code>
                <Badge variant="secondary" className="text-xs">
                  {col.data_type}
                </Badge>
                {col.is_primary_key && (
                  <Badge className="text-xs bg-amber-100 text-amber-700">PK</Badge>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
