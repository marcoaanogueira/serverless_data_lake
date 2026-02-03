import React, { useState } from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Clock, GitBranch, Loader2, Info, Sparkles, ArrowRight } from 'lucide-react';
import { cn } from "@/lib/utils";
import { SketchyButton, SketchyInput, SketchyLabel, SketchyTextarea, SketchyDivider } from '@/components/ui/sketchy';

export default function GoldJobForm({ existingJobs, onSubmit, isSubmitting }) {
  const [domain, setDomain] = useState('');
  const [jobName, setJobName] = useState('');
  const [query, setQuery] = useState('');
  const [partitionColumn, setPartitionColumn] = useState('');
  const [scheduleType, setScheduleType] = useState('cron');
  const [cronSchedule, setCronSchedule] = useState('hour');
  const [selectedDependencies, setSelectedDependencies] = useState([]);

  const handleSubmit = (e) => {
    e.preventDefault();
    const jobData = {
      domain, job_name: jobName, query, partition_column: partitionColumn,
      schedule_type: scheduleType, status: 'active'
    };
    if (scheduleType === 'cron') {
      jobData.cron_schedule = cronSchedule;
    } else {
      jobData.dependencies = selectedDependencies;
    }
    onSubmit(jobData);
    setDomain(''); setJobName(''); setQuery(''); setPartitionColumn('');
    setScheduleType('cron'); setCronSchedule('hour'); setSelectedDependencies([]);
  };

  const toggleDependency = (name) => {
    setSelectedDependencies(prev =>
      prev.includes(name) ? prev.filter(j => j !== name) : [...prev, name]
    );
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div>
        <SketchyLabel>Domain</SketchyLabel>
        <SketchyInput
          placeholder="sales, ads..."
          value={domain}
          onChange={(e) => setDomain(e.target.value)}
          required
        />
      </div>

      <div>
        <SketchyLabel>Job Name</SketchyLabel>
        <SketchyInput
          placeholder="all_vendas"
          value={jobName}
          onChange={(e) => setJobName(e.target.value)}
          className="font-mono"
          required
        />
        {jobName && (
          <p className="text-xs text-gray-400 mt-1">
            Table: <code className="text-[#5B21B6] bg-[#DDD6FE] px-1.5 py-0.5 rounded font-mono">gold.{jobName}</code>
          </p>
        )}
      </div>

      <SketchyDivider />

      <div>
        <SketchyLabel>SQL Query</SketchyLabel>
        <SketchyTextarea
          placeholder="SELECT * FROM bronze.vendas;"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          required
        />
      </div>

      <div>
        <SketchyLabel>Partition Column</SketchyLabel>
        <SketchyInput
          placeholder="created_at"
          value={partitionColumn}
          onChange={(e) => setPartitionColumn(e.target.value)}
          className="font-mono"
          required
        />
        <div className="flex items-center gap-2 mt-2 p-2 bg-[#C4B5FD] rounded-xl">
          <Info className="w-3.5 h-3.5 text-[#5B21B6]" />
          <span className="text-xs text-[#5B21B6] font-medium">Used for deduplication</span>
        </div>
      </div>

      <SketchyDivider />

      <div>
        <SketchyLabel>Schedule</SketchyLabel>
        <div className="grid grid-cols-2 gap-3">
          <button
            type="button"
            onClick={() => setScheduleType('cron')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-2xl border-2 transition-all text-left",
              scheduleType === 'cron'
                ? "bg-[#A8E6CF] border-[#6BCF9F]"
                : "bg-white border-gray-200 hover:border-gray-300"
            )}
            style={scheduleType === 'cron' ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.1)' } : {}}
          >
            <div className={cn(
              "w-10 h-10 rounded-xl flex items-center justify-center",
              scheduleType === 'cron' ? "bg-[#6BCF9F] text-white" : "bg-gray-100 text-gray-500"
            )}>
              <Clock className="w-5 h-5" />
            </div>
            <div>
              <p className="font-bold text-sm text-gray-900">Time</p>
              <p className="text-xs text-gray-500">Cron schedule</p>
            </div>
          </button>

          <button
            type="button"
            onClick={() => setScheduleType('dependency')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-2xl border-2 transition-all text-left",
              scheduleType === 'dependency'
                ? "bg-[#C4B5FD] border-[#A78BFA]"
                : "bg-white border-gray-200 hover:border-gray-300"
            )}
            style={scheduleType === 'dependency' ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.1)' } : {}}
          >
            <div className={cn(
              "w-10 h-10 rounded-xl flex items-center justify-center",
              scheduleType === 'dependency' ? "bg-[#A78BFA] text-white" : "bg-gray-100 text-gray-500"
            )}>
              <GitBranch className="w-5 h-5" />
            </div>
            <div>
              <p className="font-bold text-sm text-gray-900">Dependency</p>
              <p className="text-xs text-gray-500">After jobs</p>
            </div>
          </button>
        </div>
      </div>

      {scheduleType === 'cron' && (
        <Select value={cronSchedule} onValueChange={setCronSchedule}>
          <SelectTrigger className="border-2 border-gray-200 rounded-2xl">
            <SelectValue />
          </SelectTrigger>
          <SelectContent className="rounded-xl">
            <SelectItem value="hour">Hourly</SelectItem>
            <SelectItem value="day">Daily</SelectItem>
            <SelectItem value="month">Monthly</SelectItem>
          </SelectContent>
        </Select>
      )}

      {scheduleType === 'dependency' && (
        <div>
          {existingJobs.length === 0 ? (
            <div className="p-4 bg-gray-50 rounded-2xl border-2 border-dashed border-gray-200 text-center">
              <Sparkles className="w-6 h-6 text-[#C4B5FD] mx-auto mb-2" />
              <p className="text-sm text-gray-500 font-bold">No jobs yet</p>
            </div>
          ) : (
            <div className="space-y-2 p-3 bg-gray-50 rounded-2xl border-2 border-gray-200 max-h-40 overflow-y-auto">
              {existingJobs.filter(j => j.job_name !== jobName).map((job) => (
                <label
                  key={job.id}
                  className={cn(
                    "flex items-center gap-3 p-2.5 rounded-xl cursor-pointer transition-all",
                    selectedDependencies.includes(job.job_name)
                      ? "bg-[#DDD6FE] border border-[#C4B5FD]"
                      : "bg-white border border-transparent hover:bg-[#DDD6FE]/30"
                  )}
                >
                  <Checkbox
                    checked={selectedDependencies.includes(job.job_name)}
                    onCheckedChange={() => toggleDependency(job.job_name)}
                    className="border-2 data-[state=checked]:bg-[#A78BFA] data-[state=checked]:border-[#5B21B6]"
                  />
                  <span className="font-mono text-sm text-gray-700">{job.job_name}</span>
                </label>
              ))}
            </div>
          )}
        </div>
      )}

      <SketchyButton
        type="submit"
        disabled={isSubmitting}
        variant="lilac"
        size="lg"
        className="w-full"
      >
        {isSubmitting ? (
          <><Loader2 className="w-4 h-4 animate-spin inline mr-2" />Creating...</>
        ) : (
          <>Create Job <ArrowRight className="w-4 h-4 inline ml-2" /></>
        )}
      </SketchyButton>
    </form>
  );
}
