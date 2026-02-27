import React from 'react';
import { motion } from 'framer-motion';
import {
  Bot, Database, Search, Zap,
  BarChart3, ArrowRight, Sparkles,
  FileSearch, Brain, GitBranch,
  Server, RefreshCw, Check, ChevronRight,
  HardDrive, Globe, Layers, Lock, FileText,
  Plug, Archive, Table2,
} from 'lucide-react';
import {
  SketchyCard, SketchyButton, SketchyBadge,
  FloatingDecorations,
} from '../components/ui/sketchy';

const GITHUB_URL = 'https://github.com/marcoaanogueira/serverless_data_lake';
const openGitHub = () => window.open(GITHUB_URL, '_blank');

// ─── Navbar ────────────────────────────────────────────────────────────────
function LandingNav({ onGetStarted, onDocs }) {
  return (
    <nav className="fixed top-0 left-0 right-0 z-50 bg-white/90 backdrop-blur-sm border-b-2 border-gray-100">
      <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-xl font-black text-gray-900">
            Tadpole<span className="text-[#A8E6CF]">.</span>
          </span>
        </div>

        <div className="hidden md:flex items-center gap-8">
          <a href="#ai-agents" className="text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors">
            AI Agents
          </a>
          <a href="#features" className="text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors">
            Features
          </a>
          <a href="#architecture" className="text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors">
            Architecture
          </a>
          <button onClick={onDocs} className="text-sm font-semibold text-gray-500 hover:text-gray-900 transition-colors">
            Docs
          </button>
        </div>

        <SketchyButton
          variant="mint"
          size="sm"
          onClick={openGitHub}
          className="inline-flex items-center gap-1.5"
        >
          View on GitHub <ArrowRight className="w-4 h-4" />
        </SketchyButton>
      </div>
    </nav>
  );
}

// ─── Architecture mini-flow (Hero visual) ──────────────────────────────────
function ArchitectureFlow() {
  const steps = [
    { label: 'API',    sub: 'Any REST',  color: '#D4F5E6', border: '#A8E6CF', text: '#065F46', icon: Plug       },
    { label: 'Bronze', sub: 'Raw in S3', color: '#FEE2E2', border: '#FECACA', text: '#991B1B', icon: Archive    },
    { label: 'Silver', sub: 'Iceberg',   color: '#DDD6FE', border: '#C4B5FD', text: '#5B21B6', icon: Table2     },
    { label: 'Gold',   sub: 'dbt Models',color: '#FEF9C3', border: '#FDE68A', text: '#92400E', icon: Layers     },
    { label: 'Query',  sub: 'DuckDB',    color: '#1F2937', border: '#374151', text: '#F9FAFB', icon: Search     },
  ];

  return (
    <div
      className="bg-white rounded-3xl border-2 border-gray-200 p-6 md:p-8"
      style={{ boxShadow: '6px 8px 0 rgba(0,0,0,0.08)' }}
    >
      <div className="flex items-center justify-center gap-2 md:gap-4 flex-wrap">
        {steps.map((step, i) => (
          <React.Fragment key={step.label}>
            <div
              className="flex flex-col items-center gap-1 rounded-2xl px-4 py-3 border-2 font-bold text-sm min-w-[72px]"
              style={{ backgroundColor: step.color, borderColor: step.border, color: step.text }}
            >
              <step.icon className="w-5 h-5" style={{ color: step.text }} />
              <span className="text-xs font-black">{step.label}</span>
              <span className="text-xs font-normal opacity-70">{step.sub}</span>
            </div>
            {i < steps.length - 1 && (
              <ChevronRight className="w-4 h-4 text-gray-300 shrink-0" />
            )}
          </React.Fragment>
        ))}
      </div>
      <div className="mt-5 flex items-center justify-center gap-2">
        <div className="w-2 h-2 rounded-full bg-[#A8E6CF] animate-pulse" />
        <span className="text-xs text-gray-400 font-medium">
          Medallion Architecture · Powered by AI Agents · 100% Serverless
        </span>
        <div className="w-2 h-2 rounded-full bg-[#C4B5FD] animate-pulse" />
      </div>
    </div>
  );
}

// ─── Hero ──────────────────────────────────────────────────────────────────
function Hero({ onGetStarted, onDocs }) {
  return (
    <section className="pt-32 pb-20 px-6">
      <div className="max-w-5xl mx-auto text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="flex justify-center mb-6"
        >
          <SketchyBadge variant="mint" className="text-sm py-1.5 px-4 inline-flex items-center gap-1.5">
            <Sparkles className="w-3.5 h-3.5" />
            AI-powered · Serverless · AWS Native
          </SketchyBadge>
        </motion.div>

        <motion.h1
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1, duration: 0.6 }}
          className="text-5xl md:text-7xl font-black text-gray-900 leading-tight mb-6"
        >
          Serverless Data Platform,<br />
          <span className="text-[#A8E6CF]">Built on Open Standards.</span>
        </motion.h1>

        <motion.p
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2, duration: 0.6 }}
          className="text-xl text-gray-500 max-w-2xl mx-auto mb-10 leading-relaxed"
        >
          Data Infrastructure Without the Infrastructure. Ingest, model, and query data,
          fully serverless, built on open standards.
        </motion.p>

        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3, duration: 0.6 }}
          className="flex flex-col sm:flex-row gap-4 justify-center mb-16"
        >
          <SketchyButton
            variant="dark"
            size="lg"
            onClick={openGitHub}
            className="inline-flex items-center justify-center gap-2"
          >
            View on GitHub <ArrowRight className="w-5 h-5" />
          </SketchyButton>
          <SketchyButton variant="outline" size="lg" onClick={onDocs}>
            Read the Docs
          </SketchyButton>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 50 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4, duration: 0.7 }}
        >
          <ArchitectureFlow />
        </motion.div>
      </div>
    </section>
  );
}

// ─── AI Agents ─────────────────────────────────────────────────────────────
function AIAgentsSection() {
  const agents = [
    {
      icon: FileSearch,
      variant: 'mint',
      iconBg: '#D4F5E6',
      iconBorder: '#A8E6CF',
      iconColor: '#065F46',
      badge: 'Ingestion Agent',
      title: 'Reads your APIs so you don\'t have to.',
      description: 'Point it at an OpenAPI or Swagger doc and it handles everything from endpoint discovery to schema enrichment.',
      features: [
        'Semantic endpoint matching. maps "customer" to "person" intelligently',
        'Samples live data to auto-detect primary keys for upsert',
        'Enriches every field with AI-generated descriptions',
      ],
    },
    {
      icon: Brain,
      variant: 'lilac',
      iconBg: '#DDD6FE',
      iconBorder: '#C4B5FD',
      iconColor: '#5B21B6',
      badge: 'Transform Agent',
      title: 'Auto-generates your Gold layer.',
      description: 'Takes the metadata produced by the Ingestion Agent and builds fully-wired dbt models on top.',
      features: [
        'Uses ingestion metadata (descriptions, PKs, domains) as context',
        'Dynamically writes dbt YAML model definitions',
        'Builds the full dependency tree. no SQL writing required',
      ],
    },
    {
      icon: BarChart3,
      variant: 'peach',
      iconBg: '#FEE2E2',
      iconBorder: '#FECACA',
      iconColor: '#991B1B',
      badge: 'Analyze Agent',
      title: 'Ask questions, get SQL.',
      description: 'A ChatBI-style text-to-SQL agent that understands your schema and business context.',
      features: [
        'Cursor-style query generation. describe what you want',
        'Context-aware: knows your Bronze/Silver/Gold tables',
        'Iterates on queries based on results and feedback',
      ],
    },
  ];

  return (
    <section id="ai-agents" className="py-24 px-6 bg-gray-50">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-16">
          <SketchyBadge variant="dark" className="mb-4 inline-flex items-center gap-1.5">
            <Bot className="w-3.5 h-3.5" />
            AI Agents
          </SketchyBadge>
          <h2 className="text-4xl font-black text-gray-900 mt-4 mb-4">
            Three agents.<br />Full data lifecycle.
          </h2>
          <p className="text-lg text-gray-500 max-w-lg mx-auto">
            From raw API discovery to business insights. covered automatically.
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-6">
          {agents.map((agent, index) => (
            <motion.div
              key={agent.badge}
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.12, duration: 0.5 }}
              className="flex flex-col"
            >
              <SketchyCard variant={agent.variant} hover className="flex flex-col flex-1">
                <div
                  className="w-12 h-12 rounded-2xl flex items-center justify-center mb-4 border-2"
                  style={{ backgroundColor: agent.iconBg, borderColor: agent.iconBorder }}
                >
                  <agent.icon className="w-6 h-6" style={{ color: agent.iconColor }} />
                </div>

                <SketchyBadge variant={agent.variant} className="mb-4 self-start">
                  {agent.badge}
                </SketchyBadge>

                <h3 className="text-xl font-black text-gray-900 mb-2 leading-snug">
                  {agent.title}
                </h3>
                <p className="text-gray-600 text-sm mb-6 leading-relaxed">
                  {agent.description}
                </p>

                <ul className="space-y-3 mt-auto">
                  {agent.features.map((feature, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-gray-700">
                      <Check
                        className="w-4 h-4 mt-0.5 shrink-0"
                        style={{ color: agent.iconColor }}
                      />
                      <span>{feature}</span>
                    </li>
                  ))}
                </ul>
              </SketchyCard>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// ─── Platform Features ─────────────────────────────────────────────────────
function FeaturesSection() {
  const features = [
    {
      icon: RefreshCw,
      accentColor: '#A8E6CF',
      bgColor: '#D4F5E6',
      textColor: '#065F46',
      badge: 'Active Ingestion',
      title: 'DLT pipelines, on-demand.',
      points: [
        'Powered by DLT. runs entirely in Lambda',
        'Configurable cadence: hourly, daily, or on-demand',
        'Automatic upsert into Silver (Apache Iceberg)',
        'Full run metadata logged to S3 after each execution',
      ],
    },
    {
      icon: Database,
      accentColor: '#C4B5FD',
      bgColor: '#DDD6FE',
      textColor: '#5B21B6',
      badge: 'Passive Ingestion',
      title: 'Push data, we\'ll validate it.',
      points: [
        'REST endpoints with Pydantic schema validation',
        'Primary key auto-detection and deduplication',
        'Automatic Silver layer creation on first push',
        'Schema registry in S3. versioned and always up to date',
      ],
    },
    {
      icon: GitBranch,
      accentColor: '#FECACA',
      bgColor: '#FEE2E2',
      textColor: '#991B1B',
      badge: 'dbt Transformations',
      title: 'Gold layer, generated.',
      points: [
        'Dynamic YAML dbt model generation from metadata',
        'Schedule (Hourly/Daily/Monthly) or dependency-based orchestration',
        'Runs on ECS Fargate — fully serverless, no infra to manage',
        'Full dependency tree resolved automatically',
      ],
    },
    {
      icon: Search,
      accentColor: '#374151',
      bgColor: '#F3F4F6',
      textColor: '#1F2937',
      badge: 'Query Editor',
      title: 'Query across all layers.',
      points: [
        'Tables organized by Bronze / Silver / Gold',
        'Click any table to browse its schema catalog',
        'DuckDB on Lambda. fast, pay-per-query analytics',
        'Results feed directly into the Analyze Agent',
      ],
    },
  ];

  return (
    <section id="features" className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-16">
          <SketchyBadge variant="peach" className="mb-4 inline-flex items-center gap-1.5">
            <Zap className="w-3.5 h-3.5" />
            Platform Features
          </SketchyBadge>
          <h2 className="text-4xl font-black text-gray-900 mt-4 mb-4">
            Everything a modern<br />data team needs.
          </h2>
          <p className="text-lg text-gray-500 max-w-lg mx-auto">
            From raw API calls to polished analytics. one serverless platform.
          </p>
        </div>

        <div className="grid md:grid-cols-2 gap-6">
          {features.map((feature, index) => (
            <motion.div
              key={feature.badge}
              initial={{ opacity: 0, y: 40 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.1, duration: 0.5 }}
              className="bg-white rounded-3xl border-2 border-gray-200 p-7 hover:border-[#A8E6CF] transition-colors"
              style={{ boxShadow: '6px 8px 0 rgba(0,0,0,0.05)' }}
            >
              <div className="flex items-start gap-4">
                <div
                  className="w-11 h-11 rounded-2xl flex items-center justify-center shrink-0 border-2"
                  style={{ backgroundColor: feature.bgColor, borderColor: feature.accentColor }}
                >
                  <feature.icon className="w-5 h-5" style={{ color: feature.textColor }} />
                </div>
                <div className="flex-1">
                  <span
                    className="inline-block text-xs font-bold px-3 py-1 rounded-full border-2 mb-3"
                    style={{
                      backgroundColor: feature.bgColor,
                      borderColor: feature.accentColor,
                      color: feature.textColor,
                    }}
                  >
                    {feature.badge}
                  </span>
                  <h3 className="text-lg font-black text-gray-900 mb-4">{feature.title}</h3>
                  <ul className="space-y-2.5">
                    {feature.points.map((point, i) => (
                      <li key={i} className="flex items-start gap-2 text-sm text-gray-600">
                        <div
                          className="w-1.5 h-1.5 rounded-full mt-1.5 shrink-0"
                          style={{ backgroundColor: feature.accentColor }}
                        />
                        {point}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// ─── Architecture ──────────────────────────────────────────────────────────
function ArchitectureSection() {
  const infra = [
    { icon: HardDrive, label: 'Storage',         desc: 'S3 for all data, metadata, and YAML configs', bg: '#D4F5E6', border: '#A8E6CF', color: '#065F46' },
    { icon: Globe,     label: 'API Layer',        desc: 'FastAPI + API Gateway + Lambda',              bg: '#DDD6FE', border: '#C4B5FD', color: '#5B21B6' },
    { icon: RefreshCw, label: 'Ingestion',        desc: 'DLT on Lambda for active ingestion',          bg: '#FEE2E2', border: '#FECACA', color: '#991B1B' },
    { icon: Layers,    label: 'Silver Layer',     desc: 'Apache Iceberg with auto-dedup',              bg: '#DDD6FE', border: '#C4B5FD', color: '#5B21B6' },
    { icon: GitBranch, label: 'Transform',        desc: 'dbt on ECS Fargate. no timeout limits',      bg: '#FEE2E2', border: '#FECACA', color: '#991B1B' },
    { icon: Search,    label: 'Query',            desc: 'DuckDB on Lambda. fast & serverless',        bg: '#D4F5E6', border: '#A8E6CF', color: '#065F46' },
    { icon: Lock,      label: 'Auth',             desc: 'Secrets Manager + OIDC-ready for SSO',        bg: '#F3F4F6', border: '#D1D5DB', color: '#374151' },
    { icon: FileText,  label: 'Schema Registry',  desc: 'YAML schemas versioned in S3',                bg: '#D4F5E6', border: '#A8E6CF', color: '#065F46' },
  ];

  return (
    <section id="architecture" className="py-24 px-6 bg-gray-50">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-16">
          <SketchyBadge variant="lilac" className="mb-4 inline-flex items-center gap-1.5">
            <Server className="w-3.5 h-3.5" />
            Infrastructure
          </SketchyBadge>
          <h2 className="text-4xl font-black text-gray-900 mt-4 mb-4">
            100% Serverless.<br />Zero ops.
          </h2>
          <p className="text-lg text-gray-500 max-w-lg mx-auto">
            Every component runs serverless on AWS. Pay per use, scale to zero, no servers to manage.
          </p>
        </div>

        {/* Dark code terminal */}
        <motion.div
          initial={{ opacity: 0, y: 40 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="bg-[#111827] rounded-3xl p-8 mb-10 border-2 border-[#1F2937]"
          style={{ boxShadow: '6px 8px 0 rgba(0,0,0,0.25)' }}
        >
          {/* Fake window chrome */}
          <div className="flex items-center gap-2 mb-6">
            <div className="w-3 h-3 rounded-full bg-[#FECACA]" />
            <div className="w-3 h-3 rounded-full bg-[#FDE68A]" />
            <div className="w-3 h-3 rounded-full bg-[#A8E6CF]" />
            <span className="text-[#4B5563] text-xs font-mono ml-2">tadpole · aws architecture</span>
          </div>

          <div className="font-mono text-sm leading-6 overflow-x-auto">
            {[
              { text: 'POST /ingest/{domain}/{table}', color: '#A8E6CF', bold: true },
              { text: '      │', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { text: 'Kinesis Data Firehose', color: '#FDE68A' },
              { text: '      │', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { parts: [{ text: 'S3 Bronze', color: '#FECACA' }, { text: '  ── raw JSONL, partitioned by domain/table', color: '#4B5563' }] },
              { text: '      │', color: '#4B5563' },
              { text: '      │  (S3 event)', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { text: 'Lambda: serverless_processing_iceberg', color: '#C4B5FD' },
              { text: '      │', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { parts: [{ text: 'S3 Silver (Apache Iceberg)', color: '#93C5FD' }, { text: '  ── Glue Catalog', color: '#4B5563' }] },
              { text: '      │', color: '#4B5563' },
              { text: '      │  (Step Functions + ECS Fargate)', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { text: 'dbt transform jobs', color: '#6EE7B7' },
              { text: '      │', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { parts: [{ text: 'S3 Gold (Apache Iceberg)', color: '#FDE68A', bold: true }, { text: '  ── Glue Catalog', color: '#4B5563' }] },
              { text: '      │', color: '#4B5563' },
              { text: '      ▼', color: '#4B5563' },
              { parts: [{ text: 'DuckDB / Lambda', color: '#A8E6CF' }, { text: '  ◀────  ', color: '#4B5563' }, { text: 'GET /consumption/query', color: '#ffffff', bold: true }] },
            ].map((line, i) => (
              <div key={i} className="whitespace-pre">
                {line.parts
                  ? line.parts.map((p, j) => (
                      <span key={j} style={{ color: p.color, fontWeight: p.bold ? 'bold' : undefined }}>{p.text}</span>
                    ))
                  : <span style={{ color: line.color, fontWeight: line.bold ? 'bold' : undefined }}>{line.text}</span>
                }
              </div>
            ))}
          </div>
        </motion.div>

        {/* Infra grid */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          {infra.map((item, index) => (
            <motion.div
              key={item.label}
              initial={{ opacity: 0, scale: 0.9 }}
              whileInView={{ opacity: 1, scale: 1 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.05, duration: 0.4 }}
              className="bg-white rounded-2xl border-2 border-gray-200 p-4 text-center hover:border-[#C4B5FD] transition-colors"
              style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.05)' }}
            >
              <div
                className="w-9 h-9 rounded-xl flex items-center justify-center mx-auto mb-3 border-2"
                style={{ backgroundColor: item.bg, borderColor: item.border }}
              >
                <item.icon className="w-4 h-4" style={{ color: item.color }} />
              </div>
              <div className="font-black text-gray-900 text-sm mb-1">{item.label}</div>
              <div className="text-gray-500 text-xs leading-relaxed">{item.desc}</div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// ─── CTA ───────────────────────────────────────────────────────────────────
function CTASection({ onGetStarted, onDocs }) {
  return (
    <section className="py-24 px-6">
      <div className="max-w-4xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 40 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
        >
          <SketchyCard variant="dark" className="text-center py-16 px-8">
            <h2 className="text-4xl font-black text-white mb-4">
              Ready to build your<br />data lake?
            </h2>
            <p className="text-[#9CA3AF] text-lg mb-10 max-w-md mx-auto leading-relaxed">
              Get started in minutes. Fully serverless. Scales to zero when idle.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <SketchyButton
                variant="mint"
                size="lg"
                onClick={openGitHub}
                className="inline-flex items-center justify-center gap-2"
              >
                View on GitHub <ArrowRight className="w-5 h-5" />
              </SketchyButton>
              <SketchyButton variant="outline" size="lg" onClick={onDocs}>
                Read the Docs
              </SketchyButton>
            </div>
            <p className="text-[#4B5563] text-sm mt-8">
              Built with AWS CDK · Deployed in minutes · Open source
            </p>
          </SketchyCard>
        </motion.div>
      </div>
    </section>
  );
}

// ─── Footer ────────────────────────────────────────────────────────────────
function Footer() {
  return (
    <footer className="border-t-2 border-gray-100 py-8 px-6">
      <div className="max-w-6xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
        <div className="flex items-center gap-2">
          <span className="font-black text-gray-900">
            Tadpole<span className="text-[#A8E6CF]">.</span>
          </span>
        </div>
        <p className="text-sm text-gray-400">
          Serverless Data Lake · Medallion Architecture · AWS
        </p>
      </div>
    </footer>
  );
}

// ─── Main ──────────────────────────────────────────────────────────────────
export default function LandingPage({ onGetStarted, onDocs }) {
  return (
    <div className="min-h-screen bg-white relative">
      <FloatingDecorations />

      {/* Top gradient accent bar */}
      <div className="fixed top-0 left-0 right-0 h-0.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA] z-50" />

      <LandingNav onGetStarted={onGetStarted} onDocs={onDocs} />

      <main>
        <Hero onGetStarted={onGetStarted} onDocs={onDocs} />
        <AIAgentsSection />
        <FeaturesSection />
        <ArchitectureSection />
        <CTASection onGetStarted={onGetStarted} onDocs={onDocs} />
      </main>

      <Footer />

      {/* Bottom gradient bar */}
      <div className="fixed bottom-0 left-0 right-0 h-1.5 bg-gradient-to-r from-[#A8E6CF] via-[#C4B5FD] to-[#FECACA]" />
    </div>
  );
}
