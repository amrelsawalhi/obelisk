import React from 'react';

interface ObeliskIconProps extends React.SVGProps<SVGSVGElement> {
  className?: string;
  size?: number | string;
}

export const ObeliskIcon: React.FC<ObeliskIconProps> = ({
  className = 'w-5 h-5',
  size,
  ...props
}) => {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      width={size}
      height={size}
      {...props}
    >
      {/* Lower Foundation Slab */}
      <rect
        x="4.5"
        y="21"
        width="15"
        height="1.8"
        rx="0.6"
        fill="currentColor"
        fillOpacity="0.85"
      />
      {/* Upper Pedestal Plinth */}
      <rect
        x="6.5"
        y="19.5"
        width="11"
        height="1.5"
        rx="0.3"
        fill="currentColor"
        fillOpacity="0.95"
      />

      {/* Main Shaft - Left Face (Illuminated) */}
      <polygon
        points="9.5,6.5 8,19.5 12,19.5 12,7.5"
        fill="currentColor"
        fillOpacity="0.95"
      />
      {/* Main Shaft - Right Face (Shaded) */}
      <polygon
        points="12,7.5 12,19.5 16,19.5 14.5,6.5"
        fill="currentColor"
        fillOpacity="0.6"
      />

      {/* Pyramidion (Capstone) - Left Face */}
      <polygon
        points="12,1.5 9.5,6.5 12,7.5"
        fill="currentColor"
        fillOpacity="1"
      />
      {/* Pyramidion (Capstone) - Right Face */}
      <polygon
        points="12,1.5 12,7.5 14.5,6.5"
        fill="currentColor"
        fillOpacity="0.75"
      />

      {/* Subtle crisp highlight seam along pyramidion base */}
      <path
        d="M9.5 6.5 L12 7.5 L14.5 6.5"
        stroke="currentColor"
        strokeWidth="0.5"
        strokeOpacity="0.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};

export default ObeliskIcon;
