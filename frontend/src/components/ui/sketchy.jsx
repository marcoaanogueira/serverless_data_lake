import React from 'react';
import { cn } from "@/lib/utils";

// Simplified pastel color palette (mint, lilac, peach only)
export const colors = {
  mint: {
    bg: '#A8E6CF',
    light: '#D4F5E6',
    dark: '#6BCF9F',
    text: '#065F46',
  },
  lilac: {
    bg: '#C4B5FD',
    light: '#DDD6FE',
    dark: '#A78BFA',
    text: '#5B21B6',
  },
  peach: {
    bg: '#FECACA',
    light: '#FEE2E2',
    dark: '#FCA5A5',
    text: '#991B1B',
  },
  dark: {
    bg: '#1F2937',
    card: '#111827',
    text: '#F9FAFB',
  },
};

// Sketchy Card with bold rounded style
export const SketchyCard = ({
  children,
  className,
  variant = 'white',
  hover = true,
  ...props
}) => {
  const variants = {
    white: 'bg-white border-gray-200',
    mint: 'bg-[#A8E6CF] border-[#6BCF9F]',
    lilac: 'bg-[#C4B5FD] border-[#A78BFA]',
    peach: 'bg-[#FECACA] border-[#FCA5A5]',
    dark: 'bg-[#1F2937] border-[#374151] text-white',
  };

  return (
    <div
      className={cn(
        "relative rounded-3xl border-2 p-6 transition-all duration-300",
        variants[variant],
        hover && "hover:-translate-y-1",
        className
      )}
      style={{
        boxShadow: variant === 'dark'
          ? '0 8px 32px rgba(0,0,0,0.3)'
          : '6px 8px 0 rgba(0,0,0,0.08)',
      }}
      {...props}
    >
      {children}
    </div>
  );
};

// Bold Sketchy Button
export const SketchyButton = ({
  children,
  className,
  variant = 'mint',
  size = 'default',
  ...props
}) => {
  const variants = {
    mint: 'bg-[#A8E6CF] hover:bg-[#6BCF9F] text-[#065F46] border-[#6BCF9F]',
    lilac: 'bg-[#C4B5FD] hover:bg-[#A78BFA] text-[#5B21B6] border-[#A78BFA]',
    peach: 'bg-[#FECACA] hover:bg-[#FCA5A5] text-[#991B1B] border-[#FCA5A5]',
    dark: 'bg-[#1F2937] hover:bg-[#111827] text-white border-[#374151]',
    outline: 'bg-transparent hover:bg-gray-100 text-gray-700 border-gray-300',
    ghost: 'bg-transparent hover:bg-gray-100 text-gray-600 border-transparent',
  };

  const sizes = {
    sm: 'px-4 py-2 text-sm',
    default: 'px-6 py-3',
    lg: 'px-8 py-4 text-lg',
  };

  return (
    <button
      className={cn(
        "relative font-bold rounded-2xl border-2 transition-all duration-200",
        "active:translate-y-1 active:shadow-none",
        "disabled:opacity-50 disabled:cursor-not-allowed disabled:transform-none",
        variants[variant],
        sizes[size],
        className
      )}
      style={{
        boxShadow: '4px 4px 0 rgba(0,0,0,0.15)',
      }}
      {...props}
    >
      {children}
    </button>
  );
};

// Sketchy Input with bold style
export const SketchyInput = React.forwardRef(({
  className,
  error,
  ...props
}, ref) => {
  return (
    <input
      ref={ref}
      className={cn(
        "w-full px-4 py-3 rounded-2xl border-2 bg-white",
        "text-gray-800 placeholder-gray-400 font-medium",
        "focus:outline-none focus:ring-0 transition-all duration-200",
        error
          ? "border-[#FCA5A5] focus:border-[#FCA5A5]"
          : "border-gray-200 focus:border-[#A8E6CF]",
        className
      )}
      {...props}
    />
  );
});
SketchyInput.displayName = 'SketchyInput';

// Sketchy Textarea
export const SketchyTextarea = React.forwardRef(({
  className,
  ...props
}, ref) => {
  return (
    <textarea
      ref={ref}
      className={cn(
        "w-full px-4 py-3 rounded-2xl border-2 bg-white",
        "text-gray-800 placeholder-gray-400 font-mono text-sm",
        "focus:outline-none focus:ring-0 focus:border-[#C4B5FD]",
        "transition-all duration-200 resize-y min-h-[120px]",
        "border-gray-200",
        className
      )}
      {...props}
    />
  );
});
SketchyTextarea.displayName = 'SketchyTextarea';

// Label
export const SketchyLabel = ({ children, className, ...props }) => (
  <label
    className={cn(
      "block text-sm font-bold text-gray-700 mb-2",
      className
    )}
    {...props}
  >
    {children}
  </label>
);

// Badge with sketchy style
export const SketchyBadge = ({
  children,
  variant = 'mint',
  className,
  ...props
}) => {
  const variants = {
    mint: 'bg-[#D4F5E6] text-[#065F46] border-[#A8E6CF]',
    lilac: 'bg-[#DDD6FE] text-[#5B21B6] border-[#C4B5FD]',
    peach: 'bg-[#FEE2E2] text-[#991B1B] border-[#FECACA]',
    dark: 'bg-[#374151] text-white border-[#4B5563]',
  };

  return (
    <span
      className={cn(
        "inline-flex items-center px-3 py-1 rounded-full text-xs font-bold border-2",
        variants[variant],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
};

// Illustration wrapper component
export const Illustration = ({ src, alt, className }) => (
  <img
    src={src}
    alt={alt}
    className={cn("object-contain", className)}
    onError={(e) => {
      e.target.style.display = 'none';
    }}
  />
);

// Module card with illustration
export const ModuleCard = ({
  title,
  description,
  illustration,
  color = 'mint',
  children,
  className,
}) => {
  const bgColors = {
    mint: 'bg-[#A8E6CF]',
    lilac: 'bg-[#C4B5FD]',
    peach: 'bg-[#FECACA]',
  };

  return (
    <div
      className={cn(
        "rounded-3xl p-8 relative overflow-hidden",
        bgColors[color],
        className
      )}
      style={{ boxShadow: '6px 8px 0 rgba(0,0,0,0.1)' }}
    >
      {illustration && (
        <div className="absolute top-4 right-4 w-32 h-32 opacity-90">
          <Illustration src={illustration} alt={title} className="w-full h-full" />
        </div>
      )}
      <div className="relative z-10">
        <h2 className="text-3xl font-black text-gray-900 mb-2">{title}</h2>
        <p className="text-gray-700 font-medium max-w-md">{description}</p>
        {children}
      </div>
    </div>
  );
};

// Divider
export const SketchyDivider = ({ className }) => (
  <div className={cn("h-px bg-gray-200 my-6", className)} />
);

// Floating elements decoration
export const FloatingDecorations = () => (
  <div className="fixed inset-0 pointer-events-none overflow-hidden z-0">
    <div className="absolute top-20 left-10 w-4 h-4 rounded-full bg-[#A8E6CF] opacity-40 animate-float" />
    <div className="absolute top-40 right-20 w-3 h-3 rounded-full bg-[#C4B5FD] opacity-40 animate-float-delayed" />
    <div className="absolute bottom-40 left-20 w-5 h-5 rounded-full bg-[#FECACA] opacity-30 animate-float-slow" />
    <div className="absolute bottom-20 right-40 w-3 h-3 rounded-full bg-[#A8E6CF] opacity-40 animate-float" />
  </div>
);

// Tab button for navigation
export const TabButton = ({ active, children, onClick, color = 'mint', className }) => {
  const activeColors = {
    mint: 'bg-[#A8E6CF] text-[#065F46] border-[#6BCF9F]',
    lilac: 'bg-[#C4B5FD] text-[#5B21B6] border-[#A78BFA]',
    peach: 'bg-[#FECACA] text-[#991B1B] border-[#FCA5A5]',
  };

  return (
    <button
      onClick={onClick}
      className={cn(
        "px-6 py-3 rounded-2xl font-bold border-2 transition-all duration-200",
        active
          ? activeColors[color]
          : "bg-white text-gray-600 border-gray-200 hover:border-gray-300",
        active && "shadow-md",
        className
      )}
      style={active ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.1)' } : {}}
    >
      {children}
    </button>
  );
};
