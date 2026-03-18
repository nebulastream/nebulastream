import type { FormFieldDef } from '../../lib/sourceConfigs';

interface ConfigFormProps {
  fields: FormFieldDef[];
  values: Record<string, string>;
  onChange: (key: string, value: string) => void;
}

export default function ConfigForm({ fields, values, onChange }: ConfigFormProps) {
  return (
    <div>
      {fields.map((field) => (
        <div key={field.key} className="mb-3">
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
            {field.label}
            {field.required && <span className="text-red-500 ml-0.5">*</span>}
          </label>
          {field.type === 'select' ? (
            <select
              className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
              value={values[field.key] ?? field.defaultValue ?? ''}
              onChange={(e) => onChange(field.key, e.target.value)}
            >
              {!values[field.key] && !field.defaultValue && (
                <option value="">Select...</option>
              )}
              {field.options?.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          ) : field.type === 'textarea' ? (
            <textarea
              className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
              rows={3}
              value={values[field.key] ?? field.defaultValue ?? ''}
              placeholder={field.placeholder}
              onChange={(e) => onChange(field.key, e.target.value)}
            />
          ) : field.type === 'boolean' ? (
            <input
              type="checkbox"
              checked={(values[field.key] ?? field.defaultValue) === 'true'}
              onChange={(e) => onChange(field.key, e.target.checked ? 'true' : 'false')}
            />
          ) : (
            <input
              type={field.type}
              className="w-full px-2 py-1.5 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500"
              value={values[field.key] ?? field.defaultValue ?? ''}
              placeholder={field.placeholder}
              onChange={(e) => onChange(field.key, e.target.value)}
            />
          )}
        </div>
      ))}
    </div>
  );
}
