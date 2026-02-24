import React from 'react';
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  PieChart, Pie, Cell, ScatterChart, Scatter,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts';

const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c7c', '#8dd1e1', '#d084d0', '#ffb366'];

const CHART_MAP = {
  bar: { Chart: BarChart, Series: Bar },
  line: { Chart: LineChart, Series: Line },
  area: { Chart: AreaChart, Series: Area },
  scatter: { Chart: ScatterChart, Series: Scatter },
};

function CartesianChart({ chart_type, data, x_key, y_keys, config }) {
  const { Chart, Series } = CHART_MAP[chart_type];

  const safeConfig = config || {};
  const colors = safeConfig.colors || COLORS;
  const stacked = safeConfig.stacked || false;

  return (
    <ResponsiveContainer width="100%" height={300}>
      <Chart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis
          dataKey={x_key}
          tick={{ fontSize: 12, fill: '#6b7280' }}
          tickLine={false}
        />
        <YAxis tick={{ fontSize: 12, fill: '#6b7280' }} tickLine={false} />
        <Tooltip
          contentStyle={{
            borderRadius: '12px',
            border: '2px solid #e5e7eb',
            boxShadow: '3px 3px 0 rgba(0,0,0,0.1)',
            fontSize: 13,
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        {y_keys.map((key, i) => (
          <Series
            key={key}
            dataKey={key}
            fill={colors[i % colors.length]}
            stroke={colors[i % colors.length]}
            stackId={stacked ? 'stack' : undefined}
            fillOpacity={chart_type === 'area' ? 0.4 : 1}
          />
        ))}
      </Chart>
    </ResponsiveContainer>
  );
}

function PieChartRenderer({ data, x_key, y_keys, config }) {
  // PROTEÇÃO: Mesma coisa aqui para o PieChart
  const safeConfig = config || {};
  const colors = safeConfig.colors || COLORS;
  const valueKey = y_keys[0];

  return (
    <ResponsiveContainer width="100%" height={300}>
      <PieChart>
        <Pie
          data={data}
          dataKey={valueKey}
          nameKey={x_key}
          cx="50%"
          cy="50%"
          outerRadius={100}
          label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
          labelLine={false}
        >
          {data.map((_, i) => (
            <Cell key={i} fill={colors[i % colors.length]} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{
            borderRadius: '12px',
            border: '2px solid #e5e7eb',
            fontSize: 13,
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
      </PieChart>
    </ResponsiveContainer>
  );
}

export default function ChartRenderer({ spec }) {
  // Se o spec inteiro for null, não renderiza nada
  if (!spec) return null;

  // Extraímos os dados garantindo que config tenha um fallback inicial
  const { chart_type, title, data, x_key, y_keys, config } = spec;

  if (!data || data.length === 0) return null;

  return (
    <div className="my-3 p-4 bg-white rounded-2xl border-2 border-gray-100 w-full overflow-hidden" style={{ boxShadow: '3px 3px 0 rgba(0,0,0,0.06)' }}>
      {title && (
        <h4 className="text-sm font-bold text-gray-800 mb-3">{title}</h4>
      )}
      {chart_type === 'pie' ? (
        <PieChartRenderer data={data} x_key={x_key} y_keys={y_keys} config={config} />
      ) : CHART_MAP[chart_type] ? (
        <CartesianChart chart_type={chart_type} data={data} x_key={x_key} y_keys={y_keys} config={config} />
      ) : (
        <p className="text-sm text-gray-400">Unsupported chart type: {chart_type}</p>
      )}
    </div>
  );
}
