import { useEffect, useMemo, useState } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { api } from '../services/api';
import type { GlobalActivityPoint } from '../types/api';
import { buildGlobalActivityOption } from '../utils/globalActivity';

echarts.use([
  BarChart,
  LineChart,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  CanvasRenderer,
]);

// Fleet-wide daily volume (bars) + transaction count (line) across every
// federation — the global analogue of the federation-page activity chart.
export function GlobalActivityChart() {
  const [points, setPoints] = useState<GlobalActivityPoint[] | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api
      .getGlobalActivity(90)
      .then((data) => {
        if (!cancelled) setPoints(data);
      })
      .catch(() => {
        if (!cancelled) setError(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const option = useMemo(() => buildGlobalActivityOption(points ?? []), [points]);

  // Stay silent until we have data: an empty/failed fetch just omits the chart
  // rather than showing an error box on the landing page.
  if (error || !points || points.length === 0) {
    return null;
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6">
      <h2 className="mb-1 text-base sm:text-lg font-semibold text-gray-900 dark:text-white">
        Network activity
      </h2>
      <p className="mb-3 text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        Daily transaction volume and count across all observed federations (last 90 days).
      </p>
      <ReactEChartsCore
        echarts={echarts}
        option={option}
        notMerge
        lazyUpdate
        style={{ height: '320px', width: '100%' }}
      />
    </div>
  );
}
