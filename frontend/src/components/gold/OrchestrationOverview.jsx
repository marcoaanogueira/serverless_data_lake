import React from 'react';
import { Badge } from "@/components/ui/badge";
import { Clock, Timer, CalendarDays, CalendarRange, GitBranch, Zap } from 'lucide-react';

const SCHEDULE_CONFIG = {
  hour: { label: 'Hourly', icon: Timer, color: 'bg-blue-50 text-blue-700 border-blue-200', time: 'Every hour' },
  day: { label: 'Daily', icon: CalendarDays, color: 'bg-amber-50 text-amber-700 border-amber-200', time: '2:00 AM UTC' },
  month: { label: 'Monthly', icon: CalendarRange, color: 'bg-violet-50 text-violet-700 border-violet-200', time: '1st at 3:00 AM UTC' },
};

export default function OrchestrationOverview({ jobs }) {
  // Group cron jobs by schedule
  const cronJobs = jobs.filter(j => j.schedule_type === 'cron');
  const depJobs = jobs.filter(j => j.schedule_type === 'dependency');

  const scheduleGroups = {};
  for (const schedule of ['hour', 'day', 'month']) {
    scheduleGroups[schedule] = cronJobs.filter(j => j.cron_schedule === schedule);
  }

  // Compute effective tag for dependency jobs (mirrors backend logic)
  const depJobTags = {};
  for (const dep of depJobs) {
    // Find which cron jobs consume this dep job (highest freq)
    const consumers = cronJobs.filter(cron => {
      // A cron job "consumes" a dep job if the dep job lists the cron job as dependency
      // Actually: dep jobs depend ON cron jobs, so dep inherits tag from its dependencies
      return (dep.dependencies || []).includes(cron.job_name);
    });
    if (consumers.length > 0) {
      const freqOrder = { hour: 0, day: 1, month: 2 };
      const highestFreq = consumers.reduce((best, c) => {
        const order = freqOrder[c.cron_schedule] ?? 1;
        return order < best.order ? { schedule: c.cron_schedule, order } : best;
      }, { schedule: 'day', order: 1 });
      depJobTags[dep.job_name] = highestFreq.schedule;
    } else {
      depJobTags[dep.job_name] = 'day'; // default
    }
  }

  if (jobs.length === 0) {
    return (
      <div className="text-center py-8">
        <Zap className="w-10 h-10 text-gray-300 mx-auto mb-3" />
        <p className="text-sm text-gray-500 font-medium">No scheduled pipelines yet</p>
        <p className="text-xs text-gray-400 mt-1">Create jobs to see the orchestration overview</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Schedule Lanes */}
      {['hour', 'day', 'month'].map(schedule => {
        const config = SCHEDULE_CONFIG[schedule];
        const Icon = config.icon;
        const jobsInSchedule = scheduleGroups[schedule];
        const depInSchedule = depJobs.filter(d => depJobTags[d.job_name] === schedule);
        const totalJobs = jobsInSchedule.length + depInSchedule.length;

        if (totalJobs === 0) return null;

        return (
          <div key={schedule} className="border border-gray-200 rounded-xl p-4 bg-white">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${config.color}`}>
                  <Icon className="w-4 h-4" />
                </div>
                <div>
                  <h4 className="text-sm font-bold text-gray-900">{config.label}</h4>
                  <p className="text-xs text-gray-500">{config.time}</p>
                </div>
              </div>
              <Badge variant="outline" className="text-xs">
                {totalJobs} job{totalJobs !== 1 ? 's' : ''}
              </Badge>
            </div>

            <div className="flex flex-wrap gap-2">
              {jobsInSchedule.map(job => (
                <Badge
                  key={job.id}
                  variant="outline"
                  className={`text-xs font-mono flex items-center gap-1 ${config.color}`}
                >
                  <Clock className="w-3 h-3" />
                  {job.job_name}
                </Badge>
              ))}
              {depInSchedule.map(job => (
                <Badge
                  key={job.id}
                  variant="outline"
                  className="text-xs font-mono flex items-center gap-1 bg-purple-50 text-purple-700 border-purple-200"
                >
                  <GitBranch className="w-3 h-3" />
                  {job.job_name}
                </Badge>
              ))}
            </div>
          </div>
        );
      })}

      {/* Legend */}
      <div className="flex items-center gap-4 text-xs text-gray-500 pt-2">
        <span className="flex items-center gap-1">
          <Clock className="w-3 h-3" /> Cron-based
        </span>
        <span className="flex items-center gap-1">
          <GitBranch className="w-3 h-3" /> Dependency-based (inherits tag)
        </span>
      </div>
    </div>
  );
}
