import React, { useState } from 'react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Clock, GitBranch, Loader2, Info, Sparkles, ArrowRight, Code } from 'lucide-react';
import { cn } from "@/lib/utils";
import { SketchyButton, SketchyInput, SketchyLabel, SketchyDivider } from '@/components/ui/sketchy';

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
      domain: domain,
      job_name: jobName,
      query: query,
      partition_column: partitionColumn,
      schedule_type: scheduleType,
      status: 'active'
    };

    if (scheduleType === 'cron') {
      jobData.cron_schedule = cronSchedule;
    } else {
      jobData.dependencies = selectedDependencies;
    }

    onSubmit(jobData);

    // Reset form
    setDomain('');
    setJobName('');
    setQuery('');
    setPartitionColumn('');
    setScheduleType('cron');
    setCronSchedule('hour');
    setSelectedDependencies([]);
  };

  const toggleDependency = (jobName) => {
    setSelectedDependencies(prev =>
      prev.includes(jobName)
        ? prev.filter(j => j !== jobName)
        : [...prev, jobName]
    );
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      {/* Domain */}
      <div className="space-y-2">
        <SketchyLabel htmlFor="domain">Domain</SketchyLabel>
        <SketchyInput
          id="domain"
          placeholder="sales, ads, finance..."
          value={domain}
          onChange={(e) => setDomain(e.target.value)}
          required
        />
      </div>

      {/* Job Name */}
      <div className="space-y-2">
        <SketchyLabel htmlFor="jobName">Job Name</SketchyLabel>
        <SketchyInput
          id="jobName"
          placeholder="all_vendas"
          value={jobName}
          onChange={(e) => setJobName(e.target.value)}
          className="font-mono"
          required
        />
        {jobName && (
          <p className="text-xs text-slate-400 flex items-center gap-1">
            <ArrowRight className="w-3 h-3" />
            Table: <code className="text-[#7C3AED] font-mono bg-[#DDD6FE] px-1.5 py-0.5 rounded">gold.{jobName}</code>
          </p>
        )}
      </div>

      <SketchyDivider />

      {/* SQL Query */}
      <div className="space-y-2">
        <SketchyLabel htmlFor="query" className="flex items-center gap-2">
          <Code className="w-4 h-4 text-[#C4B5FD]" />
          SQL Query
        </SketchyLabel>
        <textarea
          id="query"
          placeholder="SELECT * FROM bronze.vendas;"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className={cn(
            "w-full px-4 py-3 rounded-xl border-2 border-slate-200",
            "bg-white text-slate-700 font-mono text-sm",
            "focus:outline-none focus:border-[#C4B5FD] focus:ring-2 focus:ring-[#C4B5FD]/30",
            "transition-all min-h-[100px] resize-y",
            "placeholder:text-slate-300"
          )}
          required
        />
      </div>

      {/* Partition Column */}
      <div className="space-y-2">
        <SketchyLabel htmlFor="partitionColumn">Partition Column</SketchyLabel>
        <SketchyInput
          id="partitionColumn"
          placeholder="created_at"
          value={partitionColumn}
          onChange={(e) => setPartitionColumn(e.target.value)}
          className="font-mono"
          required
        />
        <div className="flex items-start gap-2 p-2 bg-[#FFF9DB] rounded-lg border border-[#FBBF24]/50">
          <Info className="w-3.5 h-3.5 text-[#D97706] mt-0.5 flex-shrink-0" />
          <span className="text-xs text-[#D97706]">Used for upsert deduplication</span>
        </div>
      </div>

      <SketchyDivider />

      {/* Schedule Type Selection */}
      <div className="space-y-3">
        <SketchyLabel>Schedule Type</SketchyLabel>
        <div className="grid grid-cols-2 gap-3">
          <button
            type="button"
            onClick={() => setScheduleType('cron')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-xl border-2 transition-all text-left",
              scheduleType === 'cron'
                ? "border-[#7DD3B0] bg-[#D4F5E6]"
                : "border-slate-200 bg-white hover:border-[#A8E6CF]"
            )}
            style={scheduleType === 'cron' ? { boxShadow: '2px 3px 0 rgba(100, 116, 139, 0.1)' } : {}}
          >
            <div className={cn(
              "p-2 rounded-lg",
              scheduleType === 'cron' ? "bg-[#7DD3B0] text-white" : "bg-slate-100 text-slate-500"
            )}>
              <Clock className="w-4 h-4" />
            </div>
            <div>
              <p className="font-semibold text-sm text-slate-700">Time-based</p>
              <p className="text-xs text-slate-400">Run on schedule</p>
            </div>
          </button>

          <button
            type="button"
            onClick={() => setScheduleType('dependency')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-xl border-2 transition-all text-left",
              scheduleType === 'dependency'
                ? "border-[#A78BFA] bg-[#DDD6FE]"
                : "border-slate-200 bg-white hover:border-[#C4B5FD]"
            )}
            style={scheduleType === 'dependency' ? { boxShadow: '2px 3px 0 rgba(100, 116, 139, 0.1)' } : {}}
          >
            <div className={cn(
              "p-2 rounded-lg",
              scheduleType === 'dependency' ? "bg-[#A78BFA] text-white" : "bg-slate-100 text-slate-500"
            )}>
              <GitBranch className="w-4 h-4" />
            </div>
            <div>
              <p className="font-semibold text-sm text-slate-700">Dependency</p>
              <p className="text-xs text-slate-400">Run after jobs</p>
            </div>
          </button>
        </div>
      </div>

      {/* Cron Schedule */}
      {scheduleType === 'cron' && (
        <div className="space-y-2">
          <SketchyLabel htmlFor="cronSchedule">Frequency</SketchyLabel>
          <Select value={cronSchedule} onValueChange={setCronSchedule}>
            <SelectTrigger className="border-2 border-slate-200 rounded-xl hover:border-[#A8E6CF]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent className="rounded-xl border-2">
              <SelectItem value="hour" className="rounded-lg">Hourly</SelectItem>
              <SelectItem value="day" className="rounded-lg">Daily</SelectItem>
              <SelectItem value="month" className="rounded-lg">Monthly</SelectItem>
            </SelectContent>
          </Select>
        </div>
      )}

      {/* Dependencies */}
      {scheduleType === 'dependency' && (
        <div className="space-y-3">
          <SketchyLabel>Job Dependencies</SketchyLabel>
          {existingJobs.length === 0 ? (
            <div className="p-4 bg-slate-50 border-2 border-dashed border-slate-200 rounded-xl text-center">
              <Sparkles className="w-6 h-6 text-[#FBBF24] mx-auto mb-2" />
              <p className="text-sm text-slate-500 font-medium">No jobs available yet</p>
              <p className="text-xs text-slate-400 mt-1">Create other jobs first</p>
            </div>
          ) : (
            <div className="space-y-2 p-3 bg-slate-50 border-2 border-slate-200 rounded-xl max-h-40 overflow-y-auto">
              {existingJobs
                .filter(job => job.job_name !== jobName)
                .map((job) => (
                  <label
                    key={job.id}
                    className={cn(
                      "flex items-center gap-3 p-2.5 rounded-lg cursor-pointer transition-all",
                      selectedDependencies.includes(job.job_name)
                        ? "bg-[#DDD6FE] border border-[#C4B5FD]"
                        : "bg-white hover:bg-[#DDD6FE]/30 border border-transparent"
                    )}
                  >
                    <Checkbox
                      checked={selectedDependencies.includes(job.job_name)}
                      onCheckedChange={() => toggleDependency(job.job_name)}
                      className="border-2 data-[state=checked]:bg-[#A78BFA] data-[state=checked]:border-[#7C3AED]"
                    />
                    <span className="text-sm font-mono text-slate-700">{job.job_name}</span>
                  </label>
                ))}
            </div>
          )}
        </div>
      )}

      {/* Submit Button */}
      <SketchyButton
        type="submit"
        disabled={isSubmitting}
        variant="lilac"
        size="lg"
        className="w-full"
      >
        {isSubmitting ? (
          <span className="flex items-center gap-2">
            <Loader2 className="w-4 h-4 animate-spin" />
            Creating...
          </span>
        ) : (
          <span className="flex items-center gap-2">
            Create Job
            <ArrowRight className="w-4 h-4" />
          </span>
        )}
      </SketchyButton>
    </form>
  );
}
