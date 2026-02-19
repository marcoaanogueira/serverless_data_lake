import React, { useState, useRef, useEffect, useCallback } from 'react';
import dataLakeApi from '@/api/dataLakeClient';
import {
  Sparkles, Database, Layers, Search, ArrowRight, Loader2,
  CheckCircle2, XCircle, Link2, Globe, Tag, Play, ChevronDown, ChevronUp, Lock,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import {
  SketchyCard, SketchyButton, SketchyInput, SketchyLabel,
  SketchyBadge, SketchyDivider,
} from '@/components/ui/sketchy';

// Pipeline step status helpers
const STEP = { IDLE: 'idle', RUNNING: 'running', DONE: 'done', ERROR: 'error' };

const stepMeta = {
  extract: { label: 'Extract', icon: Database, color: 'mint', bg: '#A8E6CF', dark: '#065F46' },
  transform: { label: 'Transform', icon: Layers, color: 'lilac', bg: '#C4B5FD', dark: '#5B21B6' },
  query: { label: 'Load', icon: Search, color: 'peach', bg: '#FECACA', dark: '#991B1B' },
};

function StepIcon({ status, meta }) {
  const Icon = meta.icon;
  if (status === STEP.RUNNING) return <Loader2 className="w-5 h-5 animate-spin" style={{ color: meta.dark }} />;
  if (status === STEP.DONE) return <CheckCircle2 className="w-5 h-5 text-emerald-600" />;
  if (status === STEP.ERROR) return <XCircle className="w-5 h-5 text-red-500" />;
  return <Icon className="w-5 h-5" style={{ color: meta.dark }} />;
}

// The three-step visual pipeline header with AI branding
function PipelineHeader({ steps }) {
  const entries = ['extract', 'transform', 'query'];
  return (
    <div className="relative mb-8">
      {/* AI branding arc */}
      <div className="flex items-center justify-center gap-2 mb-4">
        <Sparkles className="w-5 h-5 text-amber-500" />
        <span className="text-sm font-black text-gray-500 tracking-widest uppercase">AI Agent</span>
        <Sparkles className="w-5 h-5 text-amber-500" />
      </div>

      {/* Steps row */}
      <div className="flex items-center justify-center gap-0">
        {entries.map((key, i) => {
          const meta = stepMeta[key];
          const status = steps[key];
          const active = status === STEP.RUNNING;
          const done = status === STEP.DONE;
          const error = status === STEP.ERROR;
          return (
            <React.Fragment key={key}>
              {i > 0 && (
                <div className="flex items-center px-1">
                  <div className={cn(
                    "w-8 h-0.5 transition-colors duration-500",
                    done || active ? "bg-gray-400" : "bg-gray-200",
                  )} />
                  <ArrowRight className={cn(
                    "w-4 h-4 -ml-1 transition-colors duration-500",
                    done || active ? "text-gray-400" : "text-gray-200",
                  )} />
                </div>
              )}
              <motion.div
                animate={active ? { scale: [1, 1.05, 1] } : {}}
                transition={active ? { repeat: Infinity, duration: 1.5 } : {}}
                className={cn(
                  "flex items-center gap-2 px-5 py-3 rounded-2xl border-2 transition-all duration-300",
                  active && "shadow-lg",
                  done && "opacity-90",
                  error && "border-red-300 bg-red-50",
                  !error && `border-2`,
                )}
                style={{
                  backgroundColor: active || done ? meta.bg : '#F9FAFB',
                  borderColor: active || done ? meta.dark : error ? undefined : '#E5E7EB',
                  boxShadow: active ? `0 4px 20px ${meta.bg}88` : undefined,
                }}
              >
                <StepIcon status={status} meta={meta} />
                <span className="font-bold text-sm" style={{ color: active || done ? meta.dark : '#6B7280' }}>
                  {meta.label}
                </span>
              </motion.div>
            </React.Fragment>
          );
        })}
      </div>

      {/* Underlining arc */}
      <div className="mx-auto mt-3 w-[85%] max-w-md">
        <svg viewBox="0 0 400 18" fill="none" className="w-full">
          <path
            d="M10 4 Q200 22 390 4"
            stroke="url(#arc-gradient)"
            strokeWidth="3"
            strokeLinecap="round"
            fill="none"
          />
          <defs>
            <linearGradient id="arc-gradient" x1="0" y1="0" x2="400" y2="0" gradientUnits="userSpaceOnUse">
              <stop offset="0%" stopColor="#A8E6CF" />
              <stop offset="50%" stopColor="#C4B5FD" />
              <stop offset="100%" stopColor="#FECACA" />
            </linearGradient>
          </defs>
        </svg>
      </div>
    </div>
  );
}

// Collapsible result panel
function ResultPanel({ title, data, variant = 'mint' }) {
  const [open, setOpen] = useState(false);
  if (!data) return null;
  const colors = {
    mint: { bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-800' },
    lilac: { bg: 'bg-purple-50', border: 'border-purple-200', text: 'text-purple-800' },
    peach: { bg: 'bg-red-50', border: 'border-red-200', text: 'text-red-800' },
  };
  const c = colors[variant];
  return (
    <div className={cn("rounded-2xl border-2 overflow-hidden", c.border, c.bg)}>
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-4 py-3 font-bold text-sm"
      >
        <span className={c.text}>{title}</span>
        {open ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
      </button>
      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-hidden"
          >
            <pre className="px-4 pb-4 text-xs font-mono whitespace-pre-wrap break-words max-h-60 overflow-y-auto">
              {typeof data === 'string' ? data : JSON.stringify(data, null, 2)}
            </pre>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

const AUTH_TYPES = [
  { value: 'none',   label: 'No Auth' },
  { value: 'bearer', label: 'Token / API Key' },
  { value: 'oauth2', label: 'OAuth2 (ROPC)' },
];

export default function AiPipeline() {
  // Form state
  const [apiUrl, setApiUrl] = useState('');
  const [baseUrl, setBaseUrl] = useState('');
  const [authType, setAuthType] = useState('none');
  const [token, setToken] = useState('');
  // OAuth2 ROPC fields
  const [oauth2TokenUrl, setOauth2TokenUrl] = useState('');
  const [oauth2ClientId, setOauth2ClientId] = useState('');
  const [oauth2ClientSecret, setOauth2ClientSecret] = useState('');
  const [oauth2Username, setOauth2Username] = useState('');
  const [oauth2Password, setOauth2Password] = useState('');
  const [interests, setInterests] = useState('');
  const [domain, setDomain] = useState('');
  const [triggerTransform, setTriggerTransform] = useState(true);

  // Pipeline state
  const [steps, setSteps] = useState({ extract: STEP.IDLE, transform: STEP.IDLE, query: STEP.IDLE });
  const [ingestionJobId, setIngestionJobId] = useState(null);
  const [transformJobId, setTransformJobId] = useState(null);
  const [ingestionResult, setIngestionResult] = useState(null);
  const [transformResult, setTransformResult] = useState(null);
  const [error, setError] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [logs, setLogs] = useState([]);

  const pollRef = useRef(null);

  const addLog = useCallback((msg) => {
    setLogs(prev => [...prev, { ts: new Date().toLocaleTimeString(), msg }]);
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  // Auto-suggest base_url from OAuth2 token URL origin when base_url is still empty.
  // e.g. token_url="https://api.instance.com/adv-service/oauth/token" → suggests "https://api.instance.com"
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (baseUrl || !oauth2TokenUrl) return;
    try {
      setBaseUrl(new URL(oauth2TokenUrl).origin);
    } catch { /* invalid URL, ignore */ }
  }, [oauth2TokenUrl]);

  const pollJob = useCallback((getJob, onComplete, onError) => {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        try {
          const job = await getJob();
          if (job.status === 'completed') {
            clearInterval(interval);
            pollRef.current = null;
            onComplete(job);
            resolve(job);
          } else if (job.status === 'failed') {
            clearInterval(interval);
            pollRef.current = null;
            onError(job.error || 'Job failed');
            resolve(null);
          }
          // else still running, keep polling
        } catch {
          // transient error, keep polling
        }
      }, 4000);
      pollRef.current = interval;
    });
  }, []);

  const runFullPipeline = async () => {
    // Validate
    if (!apiUrl.trim()) { setError('API URL is required'); return; }
    if (!interests.trim()) { setError('At least one interest is required'); return; }
    if (!domain.trim()) { setError('Domain is required'); return; }

    setError(null);
    setIsRunning(true);
    setIngestionResult(null);
    setTransformResult(null);
    setIngestionJobId(null);
    setTransformJobId(null);
    setLogs([]);
    setSteps({ extract: STEP.RUNNING, transform: STEP.IDLE, query: STEP.IDLE });

    const interestsList = interests.split(',').map(s => s.trim()).filter(Boolean);

    // Build auth payload
    const authPayload = {};
    if (authType === 'bearer') {
      authPayload.token = token.trim();
    } else if (authType === 'oauth2') {
      authPayload.oauth2 = {
        token_url: oauth2TokenUrl.trim(),
        client_id: oauth2ClientId.trim(),
        client_secret: oauth2ClientSecret.trim(),
        username: oauth2Username.trim(),
        password: oauth2Password.trim(),
      };
    }

    // === STEP 1: INGESTION ===
    addLog('Starting ingestion pipeline...');
    try {
      const ingRes = await dataLakeApi.agent.ingestion.run({
        openapi_url: apiUrl.trim(),
        ...(baseUrl.trim() ? { base_url: baseUrl.trim() } : {}),
        ...authPayload,
        interests: interestsList,
        domain: domain.trim().toLowerCase().replace(/\s+/g, '_'),
      });

      const jobId = ingRes.job_id;
      setIngestionJobId(jobId);
      addLog(`Ingestion job started: ${jobId}`);

      // Poll for completion
      const ingJob = await pollJob(
        () => dataLakeApi.agent.ingestion.getJob(jobId),
        (job) => {
          setIngestionResult(job);
          const created = job.endpoints_created?.length ?? 0;
          const skipped = job.endpoints_skipped?.length ?? 0;
          const errs    = job.setup_errors?.length ?? 0;
          addLog(`Endpoints: ${created} created, ${skipped} already existed${errs ? `, ${errs} failed` : ''}`);
          if (job.execution_arn) {
            addLog(`dlt pipeline running in ECS (SFN: ${job.execution_arn.split(':').pop()})`);
            if (job.ecs_log_group) addLog(`ECS logs → CloudWatch: ${job.ecs_log_group}`);
          } else if (job.records_loaded && Object.keys(job.records_loaded).length > 0) {
            const total = Object.values(job.records_loaded).reduce((a, b) => a + b, 0);
            addLog(`Inline pipeline: ${total} records loaded — ${JSON.stringify(job.records_loaded)}`);
          } else if (!job.execution_arn) {
            addLog('⚠ Pipeline ran inline but loaded 0 records — check Lambda logs for details');
          }
          setSteps(prev => ({ ...prev, extract: STEP.DONE }));
        },
        (err) => {
          setError(`Ingestion failed: ${err}`);
          addLog(`Ingestion failed: ${err}`);
          setSteps(prev => ({ ...prev, extract: STEP.ERROR }));
        },
      );

      if (!ingJob) { setIsRunning(false); return; }

      // === STEP 2: TRANSFORMATION ===
      if (!triggerTransform) {
        addLog('Transformation skipped (disabled by user)');
        setSteps(prev => ({ ...prev, transform: STEP.DONE, query: STEP.DONE }));
        setIsRunning(false);
        return;
      }

      setSteps(prev => ({ ...prev, transform: STEP.RUNNING }));
      addLog('Starting transformation pipeline...');

      const tables = ingJob.result?.endpoints_created || ingJob.plan?.endpoints?.map(e => e.resource_name) || [];

      const txRes = await dataLakeApi.agent.transformation.run({
        domain: domain.trim().toLowerCase().replace(/\s+/g, '_'),
        tables,
        trigger_execution: true,
      });

      const txJobId = txRes.job_id;
      setTransformJobId(txJobId);
      addLog(`Transformation job started: ${txJobId}`);

      // Poll for completion
      const txJob = await pollJob(
        () => dataLakeApi.agent.transformation.getJob(txJobId),
        (job) => {
          setTransformResult(job);
          addLog(`Transformation completed! ${job.result?.total_created || 0} jobs created`);
          setSteps(prev => ({ ...prev, transform: STEP.DONE }));
        },
        (err) => {
          setError(`Transformation failed: ${err}`);
          addLog(`Transformation failed: ${err}`);
          setSteps(prev => ({ ...prev, transform: STEP.ERROR }));
        },
      );

      if (!txJob) { setIsRunning(false); return; }

      // === STEP 3: QUERY READY ===
      setSteps(prev => ({ ...prev, query: STEP.DONE }));
      addLog('Pipeline complete! Your data is ready to query.');

    } catch (err) {
      setError(err.message || 'Pipeline failed');
      addLog(`Error: ${err.message}`);
      setSteps(prev => {
        const next = { ...prev };
        if (next.extract === STEP.RUNNING) next.extract = STEP.ERROR;
        if (next.transform === STEP.RUNNING) next.transform = STEP.ERROR;
        return next;
      });
    } finally {
      setIsRunning(false);
    }
  };

  const allDone = steps.extract === STEP.DONE && steps.transform === STEP.DONE && steps.query === STEP.DONE;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-black text-gray-900">AI Agent</h1>
        <SketchyBadge variant="dark">
          <Sparkles className="w-3 h-3 mr-1" />auto-pipeline
        </SketchyBadge>
      </div>

      {/* Pipeline visualization */}
      <SketchyCard hover={false}>
        <PipelineHeader steps={steps} />

        {/* Form */}
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <SketchyLabel>
                <Globe className="w-3.5 h-3.5 inline mr-1" />
                API URL (OpenAPI / Swagger)
              </SketchyLabel>
              <SketchyInput
                placeholder="https://swapi.dev/api/"
                value={apiUrl}
                onChange={(e) => { setApiUrl(e.target.value); setError(null); }}
                disabled={isRunning}
              />
            </div>
            <div>
              <SketchyLabel>
                <Tag className="w-3.5 h-3.5 inline mr-1" />
                Domain
              </SketchyLabel>
              <SketchyInput
                placeholder="starwars"
                value={domain}
                onChange={(e) => { setDomain(e.target.value); setError(null); }}
                disabled={isRunning}
                className="font-mono"
              />
            </div>
          </div>

          {/* Base URL override — optional, for when the spec doc host differs from the real API host */}
          <div>
            <SketchyLabel>
              <Globe className="w-3.5 h-3.5 inline mr-1" />
              API Base URL
              <span className="text-gray-400 font-normal ml-1">(optional — override spec host)</span>
            </SketchyLabel>
            <SketchyInput
              placeholder="https://minha-instancia.projurisadv.com.br/adv-service"
              value={baseUrl}
              onChange={(e) => { setBaseUrl(e.target.value); setError(null); }}
              disabled={isRunning}
            />
            <p className="text-xs text-gray-400 mt-1">
              Use when the OpenAPI spec points to a docs host but the real API runs on a different URL.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <SketchyLabel>
                <Sparkles className="w-3.5 h-3.5 inline mr-1" />
                Interests (comma separated)
              </SketchyLabel>
              <SketchyInput
                placeholder="people, planets, films"
                value={interests}
                onChange={(e) => { setInterests(e.target.value); setError(null); }}
                disabled={isRunning}
              />
            </div>
            <div>
              <SketchyLabel>
                <Lock className="w-3.5 h-3.5 inline mr-1" />
                Authentication
              </SketchyLabel>
              <div className="flex gap-1 p-1 bg-gray-100 rounded-xl border-2 border-gray-200">
                {AUTH_TYPES.map(({ value, label }) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => { setAuthType(value); setError(null); }}
                    disabled={isRunning}
                    className={cn(
                      "flex-1 py-1.5 text-xs font-bold rounded-lg transition-all",
                      authType === value
                        ? "bg-white shadow text-gray-900 border border-gray-200"
                        : "text-gray-500 hover:text-gray-700",
                    )}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Auth credentials — shown conditionally */}
          {authType === 'bearer' && (
            <div>
              <SketchyLabel>
                <Link2 className="w-3.5 h-3.5 inline mr-1" />
                Token / API Key
              </SketchyLabel>
              <SketchyInput
                placeholder="token, api key, or cookie value"
                type="password"
                value={token}
                onChange={(e) => setToken(e.target.value)}
                disabled={isRunning}
              />
              <p className="text-xs text-gray-400 mt-1">
                The agent detects the correct auth header from the API spec automatically.
              </p>
            </div>
          )}

          {authType === 'oauth2' && (
            <div className="p-4 bg-purple-50 rounded-2xl border-2 border-purple-200 space-y-3">
              <p className="text-xs font-bold text-purple-700 mb-1">
                OAuth2 Resource Owner Password Credentials
              </p>
              <div>
                <SketchyLabel>Token URL</SketchyLabel>
                <SketchyInput
                  placeholder="https://login.projurisadv.com.br/.../oauth/token"
                  value={oauth2TokenUrl}
                  onChange={(e) => setOauth2TokenUrl(e.target.value)}
                  disabled={isRunning}
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <SketchyLabel>Client ID</SketchyLabel>
                  <SketchyInput
                    placeholder="api_cliente_codigo_XXXXX"
                    value={oauth2ClientId}
                    onChange={(e) => setOauth2ClientId(e.target.value)}
                    disabled={isRunning}
                    className="font-mono text-sm"
                  />
                </div>
                <div>
                  <SketchyLabel>Client Secret</SketchyLabel>
                  <SketchyInput
                    placeholder="••••••••"
                    type="password"
                    value={oauth2ClientSecret}
                    onChange={(e) => setOauth2ClientSecret(e.target.value)}
                    disabled={isRunning}
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <SketchyLabel>
                    Username
                    <span className="text-gray-400 font-normal ml-1">(user$$tenant)</span>
                  </SketchyLabel>
                  <SketchyInput
                    placeholder="admin$$minha_empresa"
                    value={oauth2Username}
                    onChange={(e) => setOauth2Username(e.target.value)}
                    disabled={isRunning}
                    className="font-mono text-sm"
                  />
                </div>
                <div>
                  <SketchyLabel>Password</SketchyLabel>
                  <SketchyInput
                    placeholder="••••••••"
                    type="password"
                    value={oauth2Password}
                    onChange={(e) => setOauth2Password(e.target.value)}
                    disabled={isRunning}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Options row */}
          <div className="flex items-center gap-4">
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={triggerTransform}
                onChange={(e) => setTriggerTransform(e.target.checked)}
                disabled={isRunning}
                className="w-4 h-4 rounded border-2 border-gray-300 accent-[#C4B5FD]"
              />
              <span className="text-sm font-bold text-gray-600">Run Transform after Extract</span>
            </label>
          </div>

          {/* Error */}
          {error && (
            <div className="p-4 bg-[#FEE2E2] rounded-2xl border-2 border-[#FECACA] flex items-start gap-3">
              <XCircle className="w-5 h-5 text-red-500 shrink-0 mt-0.5" />
              <p className="text-[#991B1B] font-bold text-sm">{error}</p>
            </div>
          )}

          {/* Run button */}
          {!allDone && (
            <SketchyButton
              onClick={runFullPipeline}
              disabled={isRunning}
              variant="dark"
              size="lg"
              className="w-full"
            >
              {isRunning ? (
                <><Loader2 className="w-5 h-5 animate-spin inline mr-2" />Running pipeline...</>
              ) : (
                <><Sparkles className="w-5 h-5 inline mr-2" />Run AI Pipeline<Play className="w-5 h-5 inline ml-2" /></>
              )}
            </SketchyButton>
          )}

          {/* Success */}
          {allDone && (
            <motion.div
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              className="p-5 bg-emerald-50 rounded-2xl border-2 border-emerald-200 text-center"
            >
              <CheckCircle2 className="w-8 h-8 text-emerald-500 mx-auto mb-2" />
              <p className="font-black text-emerald-800 text-lg">Pipeline Complete!</p>
              <p className="text-emerald-600 text-sm mt-1">
                Your data has been extracted, transformed and is ready to query.
              </p>
              <SketchyButton
                variant="mint"
                className="mt-4"
                onClick={() => {
                  setSteps({ extract: STEP.IDLE, transform: STEP.IDLE, query: STEP.IDLE });
                  setIngestionResult(null);
                  setTransformResult(null);
                  setIngestionJobId(null);
                  setTransformJobId(null);
                  setLogs([]);
                  setError(null);
                  setAuthType('none');
                  setToken('');
                  setOauth2TokenUrl('');
                  setOauth2ClientId('');
                  setOauth2ClientSecret('');
                  setOauth2Username('');
                  setOauth2Password('');
                  setBaseUrl('');
                }}
              >
                Run Another
              </SketchyButton>
            </motion.div>
          )}
        </div>
      </SketchyCard>

      {/* Results cards */}
      {(ingestionResult || transformResult) && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {ingestionResult && (
            <div className="space-y-3">
              <ResultPanel title="Ingestion Plan" data={ingestionResult.plan} variant="mint" />
              <ResultPanel title="Ingestion Result" data={ingestionResult.result} variant="mint" />
            </div>
          )}
          {transformResult && (
            <div className="space-y-3">
              <ResultPanel title="Transformation Plan" data={transformResult.plan} variant="lilac" />
              <ResultPanel title="Transformation Result" data={transformResult.result} variant="lilac" />
            </div>
          )}
        </div>
      )}

      {/* Live logs */}
      {logs.length > 0 && (
        <SketchyCard variant="dark" hover={false} className="p-4">
          <h3 className="text-sm font-bold text-gray-400 mb-3 flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
            Pipeline Log
          </h3>
          <div className="space-y-1 max-h-40 overflow-y-auto sketchy-scrollbar font-mono text-xs">
            {logs.map((log, i) => (
              <div key={i} className="flex gap-3">
                <span className="text-gray-500 shrink-0">{log.ts}</span>
                <span className="text-gray-300">{log.msg}</span>
              </div>
            ))}
          </div>
        </SketchyCard>
      )}
    </div>
  );
}
