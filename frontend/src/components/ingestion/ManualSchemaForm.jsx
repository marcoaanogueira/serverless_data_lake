import React from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Plus, Trash2, Key } from 'lucide-react';

const DATA_TYPES = [
  { value: 'varchar', label: 'VARCHAR (Text)' },
  { value: 'integer', label: 'INTEGER' },
  { value: 'bigint', label: 'BIGINT' },
  { value: 'float', label: 'FLOAT' },
  { value: 'double', label: 'DOUBLE' },
  { value: 'boolean', label: 'BOOLEAN' },
  { value: 'date', label: 'DATE' },
  { value: 'timestamp', label: 'TIMESTAMP' },
  { value: 'json', label: 'JSON' },
];

export default function ManualSchemaForm({ columns, onColumnsChange }) {
  const addColumn = () => {
    onColumnsChange([
      ...columns,
      { column_name: '', data_type: 'varchar', required: false, is_primary_key: false }
    ]);
  };

  const removeColumn = (index) => {
    if (columns.length > 1) {
      onColumnsChange(columns.filter((_, i) => i !== index));
    }
  };

  const updateColumn = (index, field, value) => {
    const updated = columns.map((col, i) => {
      if (i === index) {
        return { ...col, [field]: value };
      }
      return col;
    });
    onColumnsChange(updated);
  };

  return (
    <div className="space-y-4">
      <div className="space-y-3">
        {columns.map((column, index) => (
          <div key={index} className="flex items-center gap-3 p-4 bg-slate-50 rounded-lg border border-slate-200">
            <div className="flex-1">
              <Input
                placeholder="column_name"
                value={column.column_name}
                onChange={(e) => updateColumn(index, 'column_name', e.target.value)}
                className="font-mono"
              />
            </div>

            <div className="w-40">
              <Select
                value={column.data_type}
                onValueChange={(value) => updateColumn(index, 'data_type', value)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DATA_TYPES.map(type => (
                    <SelectItem key={type.value} value={type.value}>
                      {type.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-2">
              <Checkbox
                id={`required-${index}`}
                checked={column.required}
                onCheckedChange={(checked) => updateColumn(index, 'required', checked)}
              />
              <label htmlFor={`required-${index}`} className="text-xs text-slate-600">
                Required
              </label>
            </div>

            <div className="flex items-center gap-2">
              <Checkbox
                id={`pk-${index}`}
                checked={column.is_primary_key}
                onCheckedChange={(checked) => updateColumn(index, 'is_primary_key', checked)}
              />
              <label htmlFor={`pk-${index}`} className="text-xs text-slate-600 flex items-center gap-1">
                <Key className="w-3 h-3" />
                PK
              </label>
            </div>

            <Button
              type="button"
              variant="ghost"
              size="icon"
              onClick={() => removeColumn(index)}
              disabled={columns.length === 1}
              className="text-red-500 hover:text-red-700 hover:bg-red-50"
            >
              <Trash2 className="w-4 h-4" />
            </Button>
          </div>
        ))}
      </div>

      <Button
        type="button"
        variant="outline"
        onClick={addColumn}
        className="w-full border-dashed"
      >
        <Plus className="w-4 h-4 mr-2" />
        Add Column
      </Button>
    </div>
  );
}
