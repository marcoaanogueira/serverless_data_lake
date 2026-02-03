import React from 'react';

// Magic Wand illustration for Auto-Inference (based on image 1)
export const MagicWandIllustration = ({ className = "w-32 h-32" }) => (
  <svg className={className} viewBox="0 0 128 128" fill="none">
    {/* Sparkles and floating elements */}
    <circle cx="20" cy="30" r="4" fill="#A8E6CF" opacity="0.7" />
    <circle cx="108" cy="40" r="3" fill="#FFB5B5" opacity="0.7" />
    <circle cx="25" cy="85" r="3" fill="#C4B5FD" opacity="0.6" />
    <circle cx="100" cy="90" r="4" fill="#FFF3B0" opacity="0.7" />

    {/* Small cubes */}
    <rect x="15" y="50" width="8" height="8" rx="1" fill="#C4B5FD" stroke="#A78BFA" strokeWidth="1.5" transform="rotate(-15 19 54)" />
    <rect x="95" cy="25" width="10" height="10" rx="1" fill="#C4B5FD" stroke="#A78BFA" strokeWidth="1.5" transform="rotate(10 100 30)" />
    <rect x="105" cy="70" width="7" height="7" rx="1" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="1.5" transform="rotate(20 108.5 73.5)" />

    {/* Wand */}
    <line x1="30" y1="100" x2="60" y2="55" stroke="#C4B5FD" strokeWidth="6" strokeLinecap="round" />
    <line x1="30" y1="100" x2="60" y2="55" stroke="#A78BFA" strokeWidth="3" strokeLinecap="round" />

    {/* Star */}
    <path d="M64 35 L68 47 L80 47 L70 55 L74 67 L64 59 L54 67 L58 55 L48 47 L60 47 Z" fill="#FFF3B0" stroke="#FBBF24" strokeWidth="2" />

    {/* Sparkle rays */}
    <line x1="64" y1="20" x2="64" y2="28" stroke="#FBBF24" strokeWidth="2" strokeLinecap="round" />
    <line x1="85" y1="51" x2="78" y2="51" stroke="#FBBF24" strokeWidth="2" strokeLinecap="round" />
    <line x1="43" y1="51" x2="50" y2="51" stroke="#FBBF24" strokeWidth="2" strokeLinecap="round" />
    <line x1="78" y1="30" x2="73" y2="36" stroke="#FBBF24" strokeWidth="2" strokeLinecap="round" />
    <line x1="50" y1="30" x2="55" y2="36" stroke="#FBBF24" strokeWidth="2" strokeLinecap="round" />

    {/* Dotted motion lines */}
    <path d="M35 75 Q45 65 55 72" stroke="#64748B" strokeWidth="1.5" strokeDasharray="3 3" fill="none" />
    <path d="M75 62 Q85 55 95 60" stroke="#64748B" strokeWidth="1.5" strokeDasharray="3 3" fill="none" />
  </svg>
);

// Data Platform illustration (based on image 2 - laptop with clouds)
export const DataPlatformIllustration = ({ className = "w-48 h-48" }) => (
  <svg className={className} viewBox="0 0 200 180" fill="none">
    {/* Background elements */}
    <circle cx="30" cy="50" r="3" fill="#C4B5FD" opacity="0.5" />
    <circle cx="170" cy="40" r="4" fill="#A8E6CF" opacity="0.5" />
    <rect x="15" y="100" width="6" height="6" fill="#FFB5B5" opacity="0.4" transform="rotate(15 18 103)" />

    {/* Clouds */}
    <path d="M50 35 C45 35 40 30 45 25 C45 18 55 18 58 22 C62 18 72 20 72 28 C78 28 80 35 75 38 C75 42 65 42 60 40 C55 44 48 42 50 35Z" fill="#C4B5FD" stroke="#A78BFA" strokeWidth="2" />
    <path d="M120 25 C116 25 112 21 116 17 C116 12 124 12 126 15 C129 12 137 14 137 20 C142 20 143 25 139 28 C139 31 131 31 127 29 C123 32 118 31 120 25Z" fill="#FFD4B8" stroke="#FDBA74" strokeWidth="2" />

    {/* Connection lines from clouds */}
    <path d="M60 42 L70 70" stroke="#A78BFA" strokeWidth="2" strokeDasharray="4 2" />
    <path d="M127 31 L115 70" stroke="#FDBA74" strokeWidth="2" strokeDasharray="4 2" />

    {/* Database left */}
    <ellipse cx="25" cy="90" rx="15" ry="6" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="2" />
    <path d="M10 90 L10 110 C10 114 17 118 25 118 C33 118 40 114 40 110 L40 90" stroke="#7DD3B0" strokeWidth="2" fill="#A8E6CF" />
    <ellipse cx="25" cy="110" rx="15" ry="6" fill="none" stroke="#7DD3B0" strokeWidth="2" />
    <path d="M40 100 L55 100" stroke="#7DD3B0" strokeWidth="2" strokeDasharray="3 2" />

    {/* Database right */}
    <ellipse cx="175" cy="90" rx="15" ry="6" fill="#FFB5B5" stroke="#FF9B9B" strokeWidth="2" />
    <path d="M160 90 L160 110 C160 114 167 118 175 118 C183 118 190 114 190 110 L190 90" stroke="#FF9B9B" strokeWidth="2" fill="#FFB5B5" />
    <ellipse cx="175" cy="110" rx="15" ry="6" fill="none" stroke="#FF9B9B" strokeWidth="2" />
    <path d="M160 100 L145 100" stroke="#FF9B9B" strokeWidth="2" strokeDasharray="3 2" />

    {/* Laptop base */}
    <path d="M55 150 L60 130 L140 130 L145 150 L55 150Z" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="2" />

    {/* Laptop screen */}
    <rect x="60" y="70" width="80" height="60" rx="4" fill="white" stroke="#7DD3B0" strokeWidth="2" />

    {/* Screen content */}
    <circle cx="78" cy="95" r="12" fill="none" stroke="#FFB5B5" strokeWidth="3" strokeDasharray="20 60" strokeDashoffset="-15" />
    <path d="M78 83 L78 95 L88 95" stroke="#FFB5B5" strokeWidth="2" strokeLinecap="round" />

    {/* Bar chart on screen */}
    <rect x="100" y="100" width="8" height="18" fill="#C4B5FD" rx="1" />
    <rect x="112" y="92" width="8" height="26" fill="#A8E6CF" rx="1" />
    <rect x="124" y="105" width="8" height="13" fill="#FFD4B8" rx="1" />

    {/* Lines on screen */}
    <line x1="68" y1="82" x2="90" y2="82" stroke="#FFD4B8" strokeWidth="2" strokeLinecap="round" />
    <line x1="100" y1="85" x2="130" y2="85" stroke="#E2E8F0" strokeWidth="2" strokeLinecap="round" />

    {/* Floating cubes */}
    <rect x="48" y="115" width="5" height="5" fill="#C4B5FD" opacity="0.6" transform="rotate(20 50.5 117.5)" />
    <rect x="147" y="110" width="6" height="6" fill="#A8E6CF" opacity="0.6" transform="rotate(-10 150 113)" />
  </svg>
);

// Analytics illustration (based on image 3 - charts with magnifying glass)
export const AnalyticsIllustration = ({ className = "w-40 h-40" }) => (
  <svg className={className} viewBox="0 0 160 160" fill="none">
    {/* Floating dots */}
    <circle cx="20" cy="30" r="4" fill="#A8E6CF" opacity="0.5" />
    <circle cx="140" cy="25" r="3" fill="#FFB5B5" opacity="0.5" />
    <circle cx="15" cy="100" r="3" fill="#C4B5FD" opacity="0.4" />
    <circle cx="145" cy="110" r="4" fill="#FFF3B0" opacity="0.5" />

    {/* Bar chart - 3D isometric style */}
    {/* Bar 1 */}
    <path d="M25 120 L25 95 L35 90 L35 115 Z" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M25 95 L35 90 L45 95 L35 100 Z" fill="#7DD3B0" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M35 90 L45 95 L45 120 L35 115 Z" fill="#D4F5E6" stroke="#7DD3B0" strokeWidth="1.5" />

    {/* Bar 2 */}
    <path d="M45 120 L45 70 L55 65 L55 115 Z" fill="#FFB5B5" stroke="#FF9B9B" strokeWidth="1.5" />
    <path d="M45 70 L55 65 L65 70 L55 75 Z" fill="#FF9B9B" stroke="#FF9B9B" strokeWidth="1.5" />
    <path d="M55 65 L65 70 L65 120 L55 115 Z" fill="#FFD4D4" stroke="#FF9B9B" strokeWidth="1.5" />

    {/* Bar 3 */}
    <path d="M65 120 L65 55 L75 50 L75 115 Z" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M65 55 L75 50 L85 55 L75 60 Z" fill="#7DD3B0" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M75 50 L85 55 L85 120 L75 115 Z" fill="#D4F5E6" stroke="#7DD3B0" strokeWidth="1.5" />

    {/* Bar 4 */}
    <path d="M85 120 L85 80 L95 75 L95 115 Z" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M85 80 L95 75 L105 80 L95 85 Z" fill="#7DD3B0" stroke="#7DD3B0" strokeWidth="1.5" />
    <path d="M95 75 L105 80 L105 120 L95 115 Z" fill="#D4F5E6" stroke="#7DD3B0" strokeWidth="1.5" />

    {/* Arrows above bars */}
    <path d="M35 85 L35 78 M32 81 L35 78 L38 81" stroke="#64748B" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M75 45 L75 38 M72 41 L75 38 L78 41" stroke="#64748B" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />

    {/* Magnifying glass */}
    <circle cx="125" cy="70" r="22" fill="none" stroke="#A8E6CF" strokeWidth="4" />
    <circle cx="125" cy="70" r="18" fill="white" stroke="#7DD3B0" strokeWidth="2" />
    <line x1="140" y1="85" x2="152" y2="100" stroke="#7DD3B0" strokeWidth="6" strokeLinecap="round" />
    <line x1="140" y1="85" x2="152" y2="100" stroke="#A8E6CF" strokeWidth="3" strokeLinecap="round" />

    {/* Eye in magnifying glass */}
    <ellipse cx="125" cy="70" rx="10" ry="7" fill="none" stroke="#64748B" strokeWidth="1.5" />
    <circle cx="125" cy="70" r="4" fill="#64748B" />
    <circle cx="126" cy="69" r="1.5" fill="white" />

    {/* Chart plate */}
    <path d="M20 135 L30 145 L90 145 L100 135 L20 135Z" fill="#FFF3B0" stroke="#FBBF24" strokeWidth="2" />
    <path d="M30 145 L30 150 L90 150 L90 145" stroke="#FBBF24" strokeWidth="2" fill="#FFF9DB" />

    {/* Line chart on plate */}
    <path d="M40 140 L50 137 L60 142 L70 135 L80 138" stroke="#64748B" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" fill="none" />
    <circle cx="40" cy="140" r="2" fill="#A8E6CF" />
    <circle cx="50" cy="137" r="2" fill="#A8E6CF" />
    <circle cx="60" cy="142" r="2" fill="#A8E6CF" />
    <circle cx="70" cy="135" r="2" fill="#A8E6CF" />
    <circle cx="80" cy="138" r="2" fill="#A8E6CF" />
  </svg>
);

// Pipeline illustration (based on image 4 - data pipeline)
export const PipelineIllustration = ({ className = "w-48 h-40" }) => (
  <svg className={className} viewBox="0 0 200 140" fill="none">
    {/* Floating cubes at input */}
    <rect x="15" y="15" width="6" height="6" fill="#C4B5FD" opacity="0.6" transform="rotate(10 18 18)" />
    <rect x="8" y="28" width="5" height="5" fill="#A8E6CF" opacity="0.5" transform="rotate(-5 10.5 30.5)" />
    <rect x="22" y="8" width="4" height="4" fill="#FFB5B5" opacity="0.5" />

    {/* Funnel */}
    <path d="M15 40 L35 40 L28 55 L22 55 Z" fill="#FFD4B8" stroke="#FDBA74" strokeWidth="2" />
    <ellipse cx="25" cy="40" rx="10" ry="4" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="2" />

    {/* Pipeline path 1 - lilac */}
    <path d="M25 55 C25 70 40 70 50 70 L80 70" stroke="#C4B5FD" strokeWidth="12" strokeLinecap="round" fill="none" />
    <path d="M25 55 C25 70 40 70 50 70 L80 70" stroke="#A78BFA" strokeWidth="2" strokeLinecap="round" fill="none" strokeDasharray="8 4" />

    {/* Processing box 1 */}
    <rect x="80" y="55" width="25" height="30" rx="3" fill="#C4B5FD" stroke="#A78BFA" strokeWidth="2" />
    <circle cx="92.5" cy="70" r="6" fill="none" stroke="#64748B" strokeWidth="2" />
    <path d="M89 70 L96 70 M92.5 67 L92.5 73" stroke="#64748B" strokeWidth="1.5" />

    {/* Pipeline path 2 - mint */}
    <path d="M105 70 L120 70 C135 70 135 45 150 45 L180 45" stroke="#A8E6CF" strokeWidth="12" strokeLinecap="round" fill="none" />
    <path d="M105 70 L120 70 C135 70 135 45 150 45 L180 45" stroke="#7DD3B0" strokeWidth="2" strokeLinecap="round" fill="none" strokeDasharray="8 4" />

    {/* Pipeline path 3 - continues down */}
    <path d="M92.5 85 L92.5 100 C92.5 115 110 115 125 115 L180 115" stroke="#A8E6CF" strokeWidth="12" strokeLinecap="round" fill="none" />
    <path d="M92.5 85 L92.5 100 C92.5 115 110 115 125 115 L180 115" stroke="#7DD3B0" strokeWidth="2" strokeLinecap="round" fill="none" strokeDasharray="8 4" />

    {/* Processing box 2 */}
    <rect x="140" y="30" width="25" height="30" rx="3" fill="#C4B5FD" stroke="#A78BFA" strokeWidth="2" />
    <circle cx="152.5" cy="45" r="6" fill="none" stroke="#64748B" strokeWidth="2" />
    <path d="M149 45 L156 45 M152.5 42 L152.5 48" stroke="#64748B" strokeWidth="1.5" />

    {/* Output box */}
    <rect x="175" y="100" width="20" height="25" rx="2" fill="#FFD4B8" stroke="#FDBA74" strokeWidth="2" />
    <text x="185" y="116" fontSize="6" fill="#64748B" textAnchor="middle" fontFamily="sans-serif">OUT</text>

    {/* Output cubes */}
    <rect x="178" y="128" width="5" height="5" fill="#A8E6CF" opacity="0.7" />
    <rect x="185" y="130" width="4" height="4" fill="#C4B5FD" opacity="0.6" />
    <rect x="191" y="127" width="5" height="5" fill="#FFB5B5" opacity="0.7" />

    {/* Arrows */}
    <path d="M170 45 L177 45 M174 42 L177 45 L174 48" stroke="#64748B" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M170 115 L175 115 M172 112 L175 115 L172 118" stroke="#64748B" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
  </svg>
);

// Server Stack illustration (based on image 5 - stacked servers)
export const ServerStackIllustration = ({ className = "w-36 h-44" }) => (
  <svg className={className} viewBox="0 0 120 150" fill="none">
    {/* Floating elements */}
    <circle cx="15" cy="30" r="3" fill="#A8E6CF" opacity="0.5" />
    <circle cx="105" cy="25" r="4" fill="#FFB5B5" opacity="0.5" />
    <circle cx="20" cy="120" r="3" fill="#C4B5FD" opacity="0.4" />
    <circle cx="100" cy="115" r="3" fill="#FFF3B0" opacity="0.5" />

    {/* Pie chart floating */}
    <circle cx="100" cy="55" r="8" fill="#FFB5B5" stroke="#FF9B9B" strokeWidth="1.5" />
    <path d="M100 55 L100 47 A8 8 0 0 1 106 59 Z" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="1" />

    {/* Bar chart floating */}
    <g transform="translate(10, 60)">
      <rect x="0" y="8" width="4" height="12" fill="#A8E6CF" rx="1" />
      <rect x="6" y="4" width="4" height="16" fill="#C4B5FD" rx="1" />
      <rect x="12" y="10" width="4" height="10" fill="#FFB5B5" rx="1" />
      <path d="M0 22 L18 22" stroke="#64748B" strokeWidth="1" />
    </g>

    {/* Base platform */}
    <ellipse cx="60" cy="140" rx="40" ry="8" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="2" />

    {/* Server 1 - bottom (coral) */}
    <rect x="30" y="115" width="60" height="25" rx="4" fill="#FFB5B5" stroke="#FF9B9B" strokeWidth="2" />
    <circle cx="42" cy="127" r="4" fill="#FF9B9B" />
    <circle cx="54" cy="127" r="4" fill="#FF9B9B" />
    <rect x="65" y="122" width="18" height="3" rx="1" fill="#FF9B9B" />
    <rect x="65" y="128" width="12" height="3" rx="1" fill="#FF9B9B" />

    {/* Server 2 - middle (cream) */}
    <rect x="30" y="85" width="60" height="28" rx="4" fill="#FFFBEB" stroke="#FBBF24" strokeWidth="2" />
    <circle cx="42" cy="99" r="5" fill="#FFF3B0" stroke="#FBBF24" strokeWidth="1.5" />
    <rect x="55" y="92" width="4" height="4" rx="1" fill="#FFB5B5" />
    <rect x="62" y="92" width="4" height="4" rx="1" fill="#FFB5B5" />
    <rect x="69" y="92" width="4" height="4" rx="1" fill="#A8E6CF" />
    <rect x="76" y="92" width="4" height="4" rx="1" fill="#A8E6CF" />
    <rect x="55" y="100" width="25" height="3" rx="1" fill="#E2E8F0" />
    <rect x="55" y="106" width="18" height="3" rx="1" fill="#E2E8F0" />

    {/* Server 3 - top section (mint + cream) */}
    <rect x="30" y="50" width="60" height="33" rx="4" fill="#A8E6CF" stroke="#7DD3B0" strokeWidth="2" />
    <rect x="34" y="54" width="52" height="25" rx="2" fill="white" stroke="#7DD3B0" strokeWidth="1" />

    {/* Top server content */}
    <circle cx="48" cy="66" r="8" fill="none" stroke="#A8E6CF" strokeWidth="3" />
    <circle cx="48" cy="66" r="8" fill="none" stroke="#7DD3B0" strokeWidth="3" strokeDasharray="15 35" strokeDashoffset="0" />
    <circle cx="48" cy="66" r="3" fill="#7DD3B0" />

    <rect x="62" y="58" width="4" height="4" rx="1" fill="#FFB5B5" />
    <rect x="68" y="58" width="4" height="4" rx="1" fill="#FFB5B5" />
    <rect x="74" y="58" width="4" height="4" rx="1" fill="#C4B5FD" />
    <rect x="62" y="65" width="4" height="4" rx="1" fill="#A8E6CF" />
    <rect x="68" y="65" width="4" height="4" rx="1" fill="#C4B5FD" />
    <rect x="74" y="65" width="4" height="4" rx="1" fill="#FFB5B5" />
    <rect x="62" y="72" width="18" height="3" rx="1" fill="#E2E8F0" />

    {/* Connection lines */}
    <path d="M15 75 Q5 85 15 95" stroke="#64748B" strokeWidth="1" strokeDasharray="2 2" fill="none" />
    <path d="M105 70 Q115 80 105 90" stroke="#64748B" strokeWidth="1" strokeDasharray="2 2" fill="none" />
  </svg>
);

// Empty state illustration
export const EmptyStateIllustration = ({ className = "w-32 h-32" }) => (
  <svg className={className} viewBox="0 0 128 128" fill="none">
    <circle cx="64" cy="64" r="40" fill="#F1F5F9" stroke="#E2E8F0" strokeWidth="2" />
    <path d="M45 60 L55 70 L75 50" stroke="#A8E6CF" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" fill="none" />
    <circle cx="64" cy="64" r="50" fill="none" stroke="#E2E8F0" strokeWidth="1" strokeDasharray="8 8" />

    <circle cx="25" cy="40" r="4" fill="#C4B5FD" opacity="0.5" />
    <circle cx="100" cy="35" r="3" fill="#FFB5B5" opacity="0.5" />
    <circle cx="30" cy="95" r="3" fill="#A8E6CF" opacity="0.5" />
    <circle cx="95" cy="90" r="4" fill="#FFF3B0" opacity="0.5" />
  </svg>
);

// Database illustration for endpoints
export const DatabaseIllustration = ({ className = "w-16 h-16", color = "mint" }) => {
  const colors = {
    mint: { fill: '#A8E6CF', stroke: '#7DD3B0', light: '#D4F5E6' },
    coral: { fill: '#FFB5B5', stroke: '#FF9B9B', light: '#FFD4D4' },
    lilac: { fill: '#C4B5FD', stroke: '#A78BFA', light: '#DDD6FE' },
  };
  const c = colors[color] || colors.mint;

  return (
    <svg className={className} viewBox="0 0 64 64" fill="none">
      <ellipse cx="32" cy="18" rx="20" ry="8" fill={c.fill} stroke={c.stroke} strokeWidth="2" />
      <path d={`M12 18 L12 46 C12 51 21 55 32 55 C43 55 52 51 52 46 L52 18`} stroke={c.stroke} strokeWidth="2" fill={c.light} />
      <ellipse cx="32" cy="46" rx="20" ry="8" fill="none" stroke={c.stroke} strokeWidth="2" />
      <ellipse cx="32" cy="32" rx="20" ry="8" fill="none" stroke={c.stroke} strokeWidth="2" strokeDasharray="4 2" opacity="0.5" />
    </svg>
  );
};
