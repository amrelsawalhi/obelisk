/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        bronze: {
          light: '#fef3c7',
          DEFAULT: '#d97706',
          dark: '#92400e',
        },
        silver: {
          light: '#f1f5f9',
          DEFAULT: '#64748b',
          dark: '#334155',
        },
        gold: {
          light: '#fef9c3',
          DEFAULT: '#ca8a04',
          dark: '#854d0e',
        },
      },
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'Menlo', 'monospace'],
      },
    },
  },
  plugins: [],
}
