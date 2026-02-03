import React from 'react';
import { cn } from "@/lib/utils";

// Pastel color palette from the design images
export const sketchyColors = {
  mint: '#A8E6CF',
  mintLight: '#D4F5E6',
  mintDark: '#7DD3B0',
  coral: '#FFB5B5',
  coralLight: '#FFD4D4',
  coralDark: '#FF9B9B',
  lilac: '#C4B5FD',
  lilacLight: '#DDD6FE',
  lilacDark: '#A78BFA',
  yellow: '#FFF3B0',
  yellowLight: '#FFF9DB',
  yellowDark: '#FBBF24',
  peach: '#FFD4B8',
  peachLight: '#FFE8D6',
  peachDark: '#FDBA74',
  slate: '#475569',
  slateLight: '#64748B',
  background: '#F8FAFC',
  backgroundDark: '#1E293B',
};

// Sketchy border SVG filter
export const SketchyFilters = () => (
  <svg width="0" height="0" style={{ position: 'absolute' }}>
    <defs>
      <filter id="sketchy-shadow">
        <feDropShadow dx="2" dy="4" stdDeviation="0" floodColor="#64748B" floodOpacity="0.15" />
      </filter>
      <filter id="sketchy-glow">
        <feGaussianBlur stdDeviation="8" result="blur" />
        <feComposite in="SourceGraphic" in2="blur" operator="over" />
      </filter>
    </defs>
  </svg>
);

// Sketchy card component with hand-drawn border effect
export const SketchyCard = ({
  children,
  className,
  color = 'white',
  hoverEffect = true,
  ...props
}) => {
  const colorStyles = {
    white: 'bg-white border-slate-300',
    mint: 'bg-[#D4F5E6] border-[#7DD3B0]',
    coral: 'bg-[#FFD4D4] border-[#FF9B9B]',
    lilac: 'bg-[#DDD6FE] border-[#A78BFA]',
    yellow: 'bg-[#FFF9DB] border-[#FBBF24]',
    peach: 'bg-[#FFE8D6] border-[#FDBA74]',
  };

  return (
    <div
      className={cn(
        "relative rounded-2xl border-2 p-6",
        "transition-all duration-300",
        colorStyles[color] || colorStyles.white,
        hoverEffect && "hover:translate-y-[-2px] hover:shadow-lg",
        className
      )}
      style={{
        boxShadow: '4px 6px 0 rgba(100, 116, 139, 0.15)',
      }}
      {...props}
    >
      {/* Hand-drawn corner decorations */}
      <svg className="absolute -top-1 -left-1 w-4 h-4 text-slate-400" viewBox="0 0 16 16">
        <path d="M2 14 Q2 2 14 2" stroke="currentColor" strokeWidth="2" fill="none" strokeLinecap="round" />
      </svg>
      <svg className="absolute -top-1 -right-1 w-4 h-4 text-slate-400" viewBox="0 0 16 16">
        <path d="M14 14 Q14 2 2 2" stroke="currentColor" strokeWidth="2" fill="none" strokeLinecap="round" />
      </svg>
      <svg className="absolute -bottom-1 -left-1 w-4 h-4 text-slate-400" viewBox="0 0 16 16">
        <path d="M2 2 Q2 14 14 14" stroke="currentColor" strokeWidth="2" fill="none" strokeLinecap="round" />
      </svg>
      <svg className="absolute -bottom-1 -right-1 w-4 h-4 text-slate-400" viewBox="0 0 16 16">
        <path d="M14 2 Q14 14 2 14" stroke="currentColor" strokeWidth="2" fill="none" strokeLinecap="round" />
      </svg>
      {children}
    </div>
  );
};

// Sketchy button with hand-drawn style
export const SketchyButton = ({
  children,
  className,
  variant = 'mint',
  size = 'default',
  ...props
}) => {
  const variantStyles = {
    mint: 'bg-[#A8E6CF] hover:bg-[#7DD3B0] text-slate-700 border-[#7DD3B0]',
    coral: 'bg-[#FFB5B5] hover:bg-[#FF9B9B] text-slate-700 border-[#FF9B9B]',
    lilac: 'bg-[#C4B5FD] hover:bg-[#A78BFA] text-slate-700 border-[#A78BFA]',
    yellow: 'bg-[#FFF3B0] hover:bg-[#FBBF24] text-slate-700 border-[#FBBF24]',
    peach: 'bg-[#FFD4B8] hover:bg-[#FDBA74] text-slate-700 border-[#FDBA74]',
    outline: 'bg-white hover:bg-slate-50 text-slate-700 border-slate-300',
    ghost: 'bg-transparent hover:bg-slate-100 text-slate-700 border-transparent',
  };

  const sizeStyles = {
    sm: 'px-3 py-1.5 text-sm',
    default: 'px-4 py-2',
    lg: 'px-6 py-3 text-lg',
  };

  return (
    <button
      className={cn(
        "relative font-medium rounded-xl border-2 transition-all duration-200",
        "active:translate-y-[2px] active:shadow-none",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        variantStyles[variant],
        sizeStyles[size],
        className
      )}
      style={{
        boxShadow: '2px 3px 0 rgba(100, 116, 139, 0.2)',
      }}
      {...props}
    >
      {children}
    </button>
  );
};

// Sketchy input with hand-drawn style
export const SketchyInput = ({
  className,
  ...props
}) => {
  return (
    <input
      className={cn(
        "w-full px-4 py-3 rounded-xl border-2 border-slate-300",
        "bg-white text-slate-700 placeholder-slate-400",
        "focus:outline-none focus:border-[#A8E6CF] focus:ring-2 focus:ring-[#A8E6CF]/30",
        "transition-all duration-200",
        className
      )}
      style={{
        boxShadow: 'inset 2px 2px 0 rgba(100, 116, 139, 0.05)',
      }}
      {...props}
    />
  );
};

// Sketchy label
export const SketchyLabel = ({ children, className, ...props }) => (
  <label
    className={cn(
      "block text-sm font-medium text-slate-600 mb-1.5",
      className
    )}
    {...props}
  >
    {children}
  </label>
);

// Floating decorative elements
export const FloatingElements = ({ className }) => (
  <div className={cn("absolute inset-0 pointer-events-none overflow-hidden", className)}>
    {/* Small cubes */}
    <svg className="absolute top-[10%] left-[5%] w-6 h-6 text-[#C4B5FD] opacity-60 animate-float" viewBox="0 0 24 24">
      <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" stroke="currentColor" strokeWidth="2" fill="none" />
    </svg>
    <svg className="absolute top-[15%] right-[8%] w-4 h-4 text-[#FFB5B5] opacity-70 animate-float-delayed" viewBox="0 0 24 24">
      <rect x="4" y="4" width="16" height="16" rx="2" stroke="currentColor" strokeWidth="2" fill="currentColor" fillOpacity="0.3" />
    </svg>
    <svg className="absolute top-[40%] left-[3%] w-5 h-5 text-[#A8E6CF] opacity-50 animate-float-slow" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="8" stroke="currentColor" strokeWidth="2" fill="currentColor" fillOpacity="0.3" />
    </svg>
    <svg className="absolute bottom-[20%] right-[5%] w-5 h-5 text-[#FFF3B0] opacity-60 animate-float" viewBox="0 0 24 24">
      <polygon points="12,2 22,22 2,22" stroke="currentColor" strokeWidth="2" fill="currentColor" fillOpacity="0.3" />
    </svg>
    <svg className="absolute bottom-[30%] left-[8%] w-4 h-4 text-[#FFD4B8] opacity-50 animate-float-delayed" viewBox="0 0 24 24">
      <path d="M12 2l3 6 6 1-4 4 1 6-6-3-6 3 1-6-4-4 6-1z" stroke="currentColor" strokeWidth="2" fill="currentColor" fillOpacity="0.3" />
    </svg>
    {/* Dots */}
    <div className="absolute top-[25%] right-[15%] w-2 h-2 rounded-full bg-[#A8E6CF] opacity-40 animate-pulse" />
    <div className="absolute top-[60%] left-[12%] w-2 h-2 rounded-full bg-[#FFB5B5] opacity-40 animate-pulse" style={{ animationDelay: '1s' }} />
    <div className="absolute bottom-[15%] right-[20%] w-2 h-2 rounded-full bg-[#C4B5FD] opacity-40 animate-pulse" style={{ animationDelay: '2s' }} />
  </div>
);

// Sketchy divider
export const SketchyDivider = ({ className }) => (
  <div className={cn("relative h-px my-6", className)}>
    <svg className="w-full h-2" preserveAspectRatio="none" viewBox="0 0 400 8">
      <path
        d="M0 4 Q50 2 100 4 T200 4 T300 4 T400 4"
        stroke="#CBD5E1"
        strokeWidth="2"
        fill="none"
        strokeLinecap="round"
      />
    </svg>
  </div>
);

// Badge with sketchy style
export const SketchyBadge = ({
  children,
  variant = 'mint',
  className,
  ...props
}) => {
  const variantStyles = {
    mint: 'bg-[#D4F5E6] text-[#059669] border-[#A8E6CF]',
    coral: 'bg-[#FFD4D4] text-[#DC2626] border-[#FFB5B5]',
    lilac: 'bg-[#DDD6FE] text-[#7C3AED] border-[#C4B5FD]',
    yellow: 'bg-[#FFF9DB] text-[#D97706] border-[#FFF3B0]',
    peach: 'bg-[#FFE8D6] text-[#EA580C] border-[#FFD4B8]',
    slate: 'bg-slate-100 text-slate-600 border-slate-300',
  };

  return (
    <span
      className={cn(
        "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border",
        variantStyles[variant],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
};

// Tab component with sketchy style
export const SketchyTabs = ({ tabs, activeTab, onTabChange, className }) => (
  <div className={cn("flex gap-2 p-1 bg-slate-100 rounded-xl", className)}>
    {tabs.map((tab) => (
      <button
        key={tab.id}
        onClick={() => onTabChange(tab.id)}
        className={cn(
          "flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg font-medium transition-all duration-200",
          activeTab === tab.id
            ? "bg-white text-slate-700 shadow-sm"
            : "text-slate-500 hover:text-slate-700"
        )}
      >
        {tab.icon}
        {tab.label}
      </button>
    ))}
  </div>
);
