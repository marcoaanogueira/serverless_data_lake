import React from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Plus, Trash2, Key } from 'lucide-react';
import { cn } from "@/lib/utils";

const DATA_TYPES = [
  { value: 'varchar', label: 'VARCHAR' },
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
    const updated = columns.map((col, i) =>
      i === index ? { ...col, [field]: value } : col
    );
    onColumnsChange(updated);
  };

  return (
    <div className="space-y-3">
      {columns.map((column, index) => (
        <div
          key={index}
          className={cn(
            "flex items-center gap-3 p-3 rounded-2xl border-2 transition-all",
            column.is_primary_key
              ? "bg-[#C4B5FD] border-[#A78BFA]"
              : "bg-white border-gray-200"
          )}
          style={{ boxShadow: '3px 3px 0 rgba(0,0,0,0.05)' }}
        >
          {/* Column name */}
          <input
            placeholder="column_name"
            value={column.column_name}
            onChange={(e) => updateColumn(index, 'column_name', e.target.value)}
            className="flex-1 px-3 py-2 rounded-xl border-2 border-gray-200 font-mono text-sm focus:border-[#A8E6CF] focus:outline-none"
          />

          {/* Data type */}
          <Select
            value={column.data_type}
            onValueChange={(value) => updateColumn(index, 'data_type', value)}
          >
            <SelectTrigger className="w-32 border-2 border-gray-200 rounded-xl">
              <SelectValue />
            </SelectTrigger>
            <SelectContent className="rounded-xl">
              {DATA_TYPES.map(type => (
                <SelectItem key={type.value} value={type.value}>
                  <span className="font-mono text-sm">{type.label}</span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Required */}
          <label className="flex items-center gap-1.5 cursor-pointer">
            <Checkbox
              checked={column.required}
              onCheckedChange={(checked) => updateColumn(index, 'required', checked)}
              className="border-2 data-[state=checked]:bg-[#A8E6CF] data-[state=checked]:border-[#6BCF9F]"
            />
            <span className="text-xs text-gray-500">Req</span>
          </label>

          {/* Primary Key */}
          <label className="flex items-center gap-1.5 cursor-pointer">
            <Checkbox
              checked={column.is_primary_key}
              onCheckedChange={(checked) => updateColumn(index, 'is_primary_key', checked)}
              className="border-2 data-[state=checked]:bg-[#C4B5FD] data-[state=checked]:border-[#A78BFA]"
            />
            <Key className={cn("w-3.5 h-3.5", column.is_primary_key ? "text-[#5B21B6]" : "text-gray-400")} />
          </label>

          {/* Delete */}
          <button
            type="button"
            onClick={() => removeColumn(index)}
            disabled={columns.length === 1}
            className={cn(
              "p-2 rounded-xl transition-colors",
              columns.length > 1
                ? "text-gray-400 hover:text-[#991B1B] hover:bg-[#FEE2E2]"
                : "text-gray-200 cursor-not-allowed"
            )}
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      ))}

      <button
        type="button"
        onClick={addColumn}
        className="w-full flex items-center justify-center gap-2 p-3 rounded-2xl border-2 border-dashed border-gray-300 text-gray-500 hover:border-[#A8E6CF] hover:text-[#065F46] hover:bg-[#D4F5E6] transition-all font-bold"
      >
        <Plus className="w-4 h-4" />
        Add Column
      </button>
    </div>
  );
}
