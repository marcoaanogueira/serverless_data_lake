import React, { useState } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Clock, GitBranch, Loader2, Info, Sparkles } from 'lucide-react';
import { cn } from "@/lib/utils";

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
    <form onSubmit={handleSubmit} className="space-y-6">
      {/* Domain */}
      <div className="space-y-2">
        <Label htmlFor="domain">Domain</Label>
        <Input
          id="domain"
          placeholder="sales, ads, finance..."
          value={domain}
          onChange={(e) => setDomain(e.target.value)}
          required
        />
        <p className="text-xs text-slate-500">
          Business domain for organizing your data
        </p>
      </div>

      {/* Job Name */}
      <div className="space-y-2">
        <Label htmlFor="jobName">Job Name</Label>
        <Input
          id="jobName"
          placeholder="all_vendas"
          value={jobName}
          onChange={(e) => setJobName(e.target.value)}
          className="font-mono"
          required
        />
        {jobName && (
          <p className="text-xs text-slate-500">
            Will create table: <code className="text-[#059669] font-mono">gold.{jobName}</code>
          </p>
        )}
      </div>

      {/* SQL Query */}
      <div className="space-y-2">
        <Label htmlFor="query">SQL Query</Label>
        <Textarea
          id="query"
          placeholder="SELECT * FROM bronze.vendas;"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="font-mono text-sm min-h-[120px]"
          required
        />
        <p className="text-xs text-slate-500">
          Write the SELECT query to populate the gold table
        </p>
      </div>

      {/* Partition Column */}
      <div className="space-y-2">
        <Label htmlFor="partitionColumn">Partition Column</Label>
        <Input
          id="partitionColumn"
          placeholder="created_at"
          value={partitionColumn}
          onChange={(e) => setPartitionColumn(e.target.value)}
          className="font-mono"
          required
        />
        <div className="flex items-start gap-2 text-xs text-slate-500">
          <Info className="w-3 h-3 mt-0.5 flex-shrink-0" />
          <span>Column used for upsert operations (deduplication)</span>
        </div>
      </div>

      {/* Schedule Type Selection */}
      <div className="space-y-3">
        <Label>Schedule Type</Label>
        <div className="grid grid-cols-2 gap-3">
          <button
            type="button"
            onClick={() => setScheduleType('cron')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-lg border-2 transition-all text-left",
              scheduleType === 'cron'
                ? "border-[#059669] bg-[#D1FAE5]"
                : "border-slate-200 bg-white hover:border-[#059669]/50"
            )}
          >
            <div className={cn(
              "p-2 rounded-lg",
              scheduleType === 'cron' ? "bg-[#059669] text-white" : "bg-slate-100"
            )}>
              <Clock className="w-4 h-4" />
            </div>
            <div>
              <p className="font-medium text-sm">Time-based</p>
              <p className="text-xs text-slate-500">Run on schedule</p>
            </div>
          </button>

          <button
            type="button"
            onClick={() => setScheduleType('dependency')}
            className={cn(
              "flex items-center gap-3 p-4 rounded-lg border-2 transition-all text-left",
              scheduleType === 'dependency'
                ? "border-[#059669] bg-[#D1FAE5]"
                : "border-slate-200 bg-white hover:border-[#059669]/50"
            )}
          >
            <div className={cn(
              "p-2 rounded-lg",
              scheduleType === 'dependency' ? "bg-[#059669] text-white" : "bg-slate-100"
            )}>
              <GitBranch className="w-4 h-4" />
            </div>
            <div>
              <p className="font-medium text-sm">Dependency-based</p>
              <p className="text-xs text-slate-500">Run after jobs</p>
            </div>
          </button>
        </div>
      </div>

      {/* Cron Schedule */}
      {scheduleType === 'cron' && (
        <div className="space-y-2">
          <Label htmlFor="cronSchedule">Frequency</Label>
          <Select value={cronSchedule} onValueChange={setCronSchedule}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="hour">Hourly</SelectItem>
              <SelectItem value="day">Daily</SelectItem>
              <SelectItem value="month">Monthly</SelectItem>
            </SelectContent>
          </Select>
        </div>
      )}

      {/* Dependencies */}
      {scheduleType === 'dependency' && (
        <div className="space-y-3">
          <Label>Job Dependencies</Label>
          {existingJobs.length === 0 ? (
            <div className="p-4 bg-slate-50 border border-slate-200 rounded-lg text-center">
              <Sparkles className="w-6 h-6 text-slate-300 mx-auto mb-2" />
              <p className="text-sm text-slate-500">No jobs available yet</p>
              <p className="text-xs text-slate-400 mt-1">Create other jobs first to set up dependencies</p>
            </div>
          ) : (
            <div className="space-y-2 p-4 bg-slate-50 border border-slate-200 rounded-lg max-h-48 overflow-y-auto">
              {existingJobs
                .filter(job => job.job_name !== jobName)
                .map((job) => (
                  <label
                    key={job.id}
                    className="flex items-center gap-3 p-2 hover:bg-white rounded-lg cursor-pointer transition-colors"
                  >
                    <Checkbox
                      checked={selectedDependencies.includes(job.job_name)}
                      onCheckedChange={() => toggleDependency(job.job_name)}
                    />
                    <span className="text-sm font-mono text-slate-700">{job.job_name}</span>
                  </label>
                ))}
            </div>
          )}
          <p className="text-xs text-slate-500">
            This job will run after all selected jobs complete
          </p>
        </div>
      )}

      {/* Submit Button */}
      <Button
        type="submit"
        disabled={isSubmitting}
        className="w-full bg-[#059669] hover:bg-[#047857]"
      >
        {isSubmitting ? (
          <>
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            Creating Job...
          </>
        ) : (
          'Create Gold Job'
        )}
      </Button>
    </form>
  );
}
