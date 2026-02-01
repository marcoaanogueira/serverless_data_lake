import React, { useMemo } from 'react';
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { GitBranch, Maximize2 } from 'lucide-react';

function GraphVisualization({ graph, maxX, maxY }) {
  return (
    <svg
      width={Math.max(800, maxX)}
      height={Math.max(400, maxY)}
      className="mx-auto"
    >
      {/* Define arrow marker */}
      <defs>
        <marker
          id="arrowhead"
          markerWidth="10"
          markerHeight="10"
          refX="5"
          refY="5"
          orient="auto"
        >
          <polygon
            points="0 0, 10 5, 0 10"
            fill="#059669"
          />
        </marker>
      </defs>

      {/* Draw edges */}
      {graph.edges.map((edge, idx) => {
        const fromNode = graph.nodes.find(n => n.id === edge.from);
        const toNode = graph.nodes.find(n => n.id === edge.to);

        if (!fromNode || !toNode) return null;

        const x1 = fromNode.x + 100;
        const y1 = fromNode.y + 80;
        const x2 = toNode.x + 100;
        const y2 = toNode.y;

        const offset = 60;
        const path = `M ${x1} ${y1} C ${x1} ${y1 + offset}, ${x2} ${y2 - offset}, ${x2} ${y2}`;

        return (
          <path
            key={idx}
            d={path}
            stroke="#059669"
            strokeWidth="2"
            fill="none"
            markerEnd="url(#arrowhead)"
          />
        );
      })}

      {/* Draw nodes */}
      {graph.nodes.map((node, idx) => (
        <g key={idx}>
          <rect
            x={node.x}
            y={node.y}
            width="200"
            height="80"
            rx="8"
            fill="white"
            stroke={node.scheduleType === 'dependency' ? '#059669' : '#6B7280'}
            strokeWidth="2"
            className="drop-shadow-md"
          />

          <text
            x={node.x + 100}
            y={node.y + 30}
            textAnchor="middle"
            className="text-sm font-mono font-semibold fill-[#111827]"
          >
            {node.label.length > 20 ? node.label.substring(0, 18) + '...' : node.label}
          </text>

          {node.scheduleType === 'cron' ? (
            <text
              x={node.x + 100}
              y={node.y + 55}
              textAnchor="middle"
              className="text-xs fill-[#6B7280]"
            >
              {node.cronSchedule}ly
            </text>
          ) : (
            <text
              x={node.x + 100}
              y={node.y + 55}
              textAnchor="middle"
              className="text-xs fill-[#059669]"
            >
              {node.dependencies.length} dependencies
            </text>
          )}

          <text
            x={node.x + 100}
            y={node.y + 70}
            textAnchor="middle"
            className="text-xs fill-gray-400 capitalize"
          >
            {node.domain}
          </text>
        </g>
      ))}
    </svg>
  );
}

export default function DependencyGraph({ jobs }) {
  const graph = useMemo(() => {
    if (!jobs || jobs.length === 0) return { nodes: [], edges: [] };

    const nodes = jobs.map((job) => ({
      id: job.job_name,
      label: job.job_name,
      domain: job.domain,
      scheduleType: job.schedule_type,
      cronSchedule: job.cron_schedule,
      dependencies: job.dependencies || [],
      x: 0,
      y: 0,
      level: 0
    }));

    const edges = [];
    jobs.forEach(job => {
      if (job.dependencies && job.dependencies.length > 0) {
        job.dependencies.forEach(dep => {
          edges.push({
            from: dep,
            to: job.job_name
          });
        });
      }
    });

    const nodeLevels = new Map();
    nodes.forEach(node => nodeLevels.set(node.id, 0));

    let changed = true;
    while (changed) {
      changed = false;
      nodes.forEach(node => {
        if (node.dependencies && node.dependencies.length > 0) {
          const maxDepLevel = Math.max(
            ...node.dependencies.map(dep => nodeLevels.get(dep) || 0)
          );
          const newLevel = maxDepLevel + 1;
          if (nodeLevels.get(node.id) < newLevel) {
            nodeLevels.set(node.id, newLevel);
            changed = true;
          }
        }
      });
    }

    nodes.forEach(node => {
      node.level = nodeLevels.get(node.id) || 0;
    });

    const minLevel = Math.min(...nodes.map(n => n.level));
    nodes.forEach(node => {
      node.level = node.level - minLevel;
    });

    const levelGroups = nodes.reduce((acc, node) => {
      if (!acc[node.level]) acc[node.level] = [];
      acc[node.level].push(node);
      return acc;
    }, {});

    const nodeWidth = 200;
    const nodeHeight = 80;
    const horizontalSpacing = 80;
    const verticalSpacing = 150;

    Object.entries(levelGroups).forEach(([level, nodesInLevel]) => {
      const levelIndex = parseInt(level);
      const startX = 50;

      nodesInLevel.forEach((node, index) => {
        node.x = startX + index * (nodeWidth + horizontalSpacing);
        node.y = 50 + levelIndex * (nodeHeight + verticalSpacing);
      });
    });

    return { nodes, edges };
  }, [jobs]);

  if (jobs.length === 0) {
    return (
      <div className="text-center py-16">
        <GitBranch className="w-12 h-12 text-slate-300 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-slate-700 mb-2">No jobs to visualize</h3>
        <p className="text-slate-500 text-sm">Create jobs with dependencies to see the graph</p>
      </div>
    );
  }

  const dependencyJobs = jobs.filter(j => j.schedule_type === 'dependency' && j.dependencies?.length > 0);

  if (dependencyJobs.length === 0) {
    return (
      <div className="text-center py-16">
        <GitBranch className="w-12 h-12 text-slate-300 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-slate-700 mb-2">No dependency-based jobs</h3>
        <p className="text-slate-500 text-sm">Create jobs with dependencies to see the dependency graph</p>
      </div>
    );
  }

  const maxX = Math.max(...graph.nodes.map(n => n.x)) + 250;
  const maxY = Math.max(...graph.nodes.map(n => n.y)) + 150;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-[#111827] mb-2">Dependency Graph</h3>
          <p className="text-sm text-[#6B7280]">Visual representation of job dependencies</p>
        </div>
        <Dialog>
          <DialogTrigger asChild>
            <Button variant="outline" size="sm">
              <Maximize2 className="w-4 h-4 mr-2" />
              Expand
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-[95vw] h-[95vh]">
            <DialogHeader>
              <DialogTitle>Dependency Graph - Full View</DialogTitle>
            </DialogHeader>
            <div className="overflow-auto bg-[#D1FAE5]/30 rounded-lg p-8 h-full border border-gray-200">
              <GraphVisualization graph={graph} maxX={maxX} maxY={maxY} />
            </div>
          </DialogContent>
        </Dialog>
      </div>

      <div className="overflow-auto bg-[#D1FAE5]/30 rounded-lg p-8 max-h-96 border border-gray-200">
        <GraphVisualization graph={graph} maxX={maxX} maxY={maxY} />
      </div>

      {/* Legend */}
      <div className="mt-6 flex items-center gap-6 text-sm">
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 border-2 border-[#059669] rounded bg-white" />
          <span className="text-slate-600">Dependency-based</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 border-2 border-[#6B7280] rounded bg-white" />
          <span className="text-slate-600">Time-based (cron)</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-6 h-0.5 bg-[#059669]" />
          <svg width="10" height="10">
            <polygon points="0 0, 10 5, 0 10" fill="#059669" />
          </svg>
          <span className="text-slate-600">Dependency flow</span>
        </div>
      </div>
    </div>
  );
}
