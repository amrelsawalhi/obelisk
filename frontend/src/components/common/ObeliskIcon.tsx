import React from 'react';

interface ObeliskIconProps extends React.SVGProps<SVGSVGElement> {
  className?: string;
  size?: number | string;
  /** When true, uses fill="currentColor" with tiered opacities instead of the multi-tone sky palette */
  monochrome?: boolean;
}

export const ObeliskIcon: React.FC<ObeliskIconProps> = ({
  className = 'w-5 h-5',
  size,
  monochrome = false,
  ...props
}) => {
  return (
    <svg
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      width={size}
      height={size}
      {...props}
    >
      {/* Left Face: Pyramidion Capstone (Lit) */}
      <polygon
        points="16,2 16,7 13.5,7"
        fill={monochrome ? 'currentColor' : '#7dd3fc'}
        fillOpacity={monochrome ? 1 : undefined}
      />
      {/* Left Face: Tapered Shaft (Lit) */}
      <polygon
        points="16,8 16,27 11,27 13.5,8"
        fill={monochrome ? 'currentColor' : '#38bdf8'}
        fillOpacity={monochrome ? 0.85 : undefined}
      />

      {/* Right Face: Pyramidion Capstone (Shadow) */}
      <polygon
        points="16,2 18.5,7 16,7"
        fill={monochrome ? 'currentColor' : '#0284c7'}
        fillOpacity={monochrome ? 0.75 : undefined}
      />
      {/* Right Face: Tapered Shaft (Shadow) */}
      <polygon
        points="16,8 18.5,8 21,27 16,27"
        fill={monochrome ? 'currentColor' : '#0369a1'}
        fillOpacity={monochrome ? 0.6 : undefined}
      />

      {/* Stepped Plinth Foundation */}
      <rect
        x="9"
        y="28"
        width="14"
        height="2"
        rx="0.5"
        fill={monochrome ? 'currentColor' : '#0284c7'}
        fillOpacity={monochrome ? 0.9 : undefined}
      />
    </svg>
  );
};

export default ObeliskIcon;
