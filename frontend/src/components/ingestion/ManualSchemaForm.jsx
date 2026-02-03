import React from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Plus, Trash2, Key, GripVertical } from 'lucide-react';
import { cn } from "@/lib/utils";

const DATA_TYPES = [
  { value: 'varchar', label: 'VARCHAR', color: 'text-[#059669]' },
  { value: 'integer', label: 'INTEGER', color: 'text-[#7C3AED]' },
  { value: 'bigint', label: 'BIGINT', color: 'text-[#7C3AED]' },
  { value: 'float', label: 'FLOAT', color: 'text-[#D97706]' },
  { value: 'double', label: 'DOUBLE', color: 'text-[#D97706]' },
  { value: 'boolean', label: 'BOOLEAN', color: 'text-[#DC2626]' },
  { value: 'date', label: 'DATE', color: 'text-[#0891B2]' },
  { value: 'timestamp', label: 'TIMESTAMP', color: 'text-[#0891B2]' },
  { value: 'json', label: 'JSON', color: 'text-[#EA580C]' },
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
      {/* Header */}
      <div className="flex items-center gap-3 px-4 text-xs font-medium text-slate-400 uppercase tracking-wide">
        <div className="w-6" />
        <div className="flex-1">Column Name</div>
        <div className="w-36">Type</div>
        <div className="w-20 text-center">Required</div>
        <div className="w-16 text-center">PK</div>
        <div className="w-10" />
      </div>

      {/* Columns */}
      <div className="space-y-2">
        {columns.map((column, index) => (
          <div
            key={index}
            className={cn(
              "flex items-center gap-3 p-3 rounded-xl border-2 transition-all",
              "bg-white border-slate-200 hover:border-[#A8E6CF]",
              column.is_primary_key && "bg-[#FFF9DB] border-[#FBBF24]"
            )}
            style={{ boxShadow: '2px 3px 0 rgba(100, 116, 139, 0.05)' }}
          >
            {/* Drag handle */}
            <div className="w-6 text-slate-300 cursor-grab">
              <GripVertical className="w-4 h-4" />
            </div>

            {/* Column name input */}
            <div className="flex-1">
              <input
                placeholder="column_name"
                value={column.column_name}
                onChange={(e) => updateColumn(index, 'column_name', e.target.value)}
                className={cn(
                  "w-full px-3 py-2 rounded-lg border-2 border-slate-200",
                  "font-mono text-sm bg-white",
                  "focus:outline-none focus:border-[#A8E6CF] focus:ring-2 focus:ring-[#A8E6CF]/30",
                  "transition-all placeholder:text-slate-300"
                )}
              />
            </div>

            {/* Data type select */}
            <div className="w-36">
              <Select
                value={column.data_type}
                onValueChange={(value) => updateColumn(index, 'data_type', value)}
              >
                <SelectTrigger className="border-2 border-slate-200 rounded-lg hover:border-[#C4B5FD] focus:border-[#C4B5FD]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="rounded-xl border-2 border-slate-200">
                  {DATA_TYPES.map(type => (
                    <SelectItem key={type.value} value={type.value} className="rounded-lg">
                      <span className={cn("font-mono text-sm", type.color)}>
                        {type.label}
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Required checkbox */}
            <div className="w-20 flex justify-center">
              <label className="flex items-center gap-2 cursor-pointer">
                <Checkbox
                  id={`required-${index}`}
                  checked={column.required}
                  onCheckedChange={(checked) => updateColumn(index, 'required', checked)}
                  className="border-2 border-slate-300 data-[state=checked]:bg-[#A8E6CF] data-[state=checked]:border-[#7DD3B0]"
                />
                <span className="text-xs text-slate-500 hidden sm:inline">Req</span>
              </label>
            </div>

            {/* Primary key checkbox */}
            <div className="w-16 flex justify-center">
              <label className="flex items-center gap-1.5 cursor-pointer">
                <Checkbox
                  id={`pk-${index}`}
                  checked={column.is_primary_key}
                  onCheckedChange={(checked) => updateColumn(index, 'is_primary_key', checked)}
                  className="border-2 border-slate-300 data-[state=checked]:bg-[#FBBF24] data-[state=checked]:border-[#D97706]"
                />
                <Key className={cn(
                  "w-3.5 h-3.5 transition-colors",
                  column.is_primary_key ? "text-[#D97706]" : "text-slate-400"
                )} />
              </label>
            </div>

            {/* Delete button */}
            <div className="w-10">
              <button
                type="button"
                onClick={() => removeColumn(index)}
                disabled={columns.length === 1}
                className={cn(
                  "p-2 rounded-lg transition-all",
                  columns.length > 1
                    ? "text-[#FF9B9B] hover:bg-[#FFD4D4] hover:text-[#DC2626]"
                    : "text-slate-200 cursor-not-allowed"
                )}
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Add column button */}
      <button
        type="button"
        onClick={addColumn}
        className={cn(
          "w-full flex items-center justify-center gap-2 p-3 rounded-xl",
          "border-2 border-dashed border-slate-300 text-slate-500",
          "hover:border-[#A8E6CF] hover:text-[#059669] hover:bg-[#D4F5E6]/30",
          "transition-all"
        )}
      >
        <Plus className="w-4 h-4" />
        <span className="font-medium">Add Column</span>
      </button>
    </div>
  );
}
