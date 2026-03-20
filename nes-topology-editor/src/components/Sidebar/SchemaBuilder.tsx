import type { SchemaField } from '../../lib/types';

const NES_TYPES = [
  'INT8', 'INT16', 'INT32', 'INT64',
  'UINT8', 'UINT16', 'UINT32', 'UINT64',
  'FLOAT32', 'FLOAT64', 'BOOLEAN', 'CHAR', 'VARSIZED',
] as const;

interface SchemaBuilderProps {
  fields: SchemaField[];
  onChange: (fields: SchemaField[]) => void;
}

export default function SchemaBuilder({ fields, onChange }: SchemaBuilderProps) {
  const handleAddField = () => {
    onChange([...fields, { name: '', type: 'INT32', nullable: false }]);
  };

  const handleRemoveField = (index: number) => {
    onChange(fields.filter((_, i) => i !== index));
  };

  const handleNameChange = (index: number, name: string) => {
    onChange(fields.map((f, i) => (i === index ? { ...f, name } : f)));
  };

  const handleTypeChange = (index: number, type: string) => {
    onChange(fields.map((f, i) => (i === index ? { ...f, type } : f)));
  };

  const handleNullableChange = (index: number, nullable: boolean) => {
    onChange(fields.map((f, i) => (i === index ? { ...f, nullable } : f)));
  };

  return (
    <div className="mt-2">
      {fields.length === 0 ? (
        <p className="text-gray-400 text-sm text-center py-2">No fields defined</p>
      ) : (
        <div className="flex flex-col gap-2">
          {fields.map((field, index) => (
            <div key={index} className="flex items-center gap-1 min-w-0">
              <input
                type="text"
                value={field.name}
                onChange={(e) => handleNameChange(index, e.target.value)}
                placeholder="Field name"
                className="min-w-0 flex-1 text-sm border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded px-2 py-1"
              />
              <select
                value={field.type}
                onChange={(e) => handleTypeChange(index, e.target.value)}
                className="w-20 text-xs border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded px-1 py-1"
              >
                {NES_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
              <input
                type="checkbox"
                checked={field.nullable ?? false}
                onChange={(e) => handleNullableChange(index, e.target.checked)}
                title="Nullable"
                className="shrink-0"
              />
              <button
                type="button"
                aria-label="Remove"
                onClick={() => handleRemoveField(index)}
                className="shrink-0 text-red-500 hover:text-red-700 text-sm font-bold px-1"
              >
                -
              </button>
            </div>
          ))}
        </div>
      )}
      <button
        type="button"
        aria-label="Add"
        onClick={handleAddField}
        className="mt-2 text-blue-600 hover:text-blue-800 text-sm font-medium"
      >
        + Add Field
      </button>
    </div>
  );
}
