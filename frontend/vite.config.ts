import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        // Monaco and xterm are only needed by the editor and terminal routes. Giving
        // them their own chunks keeps them out of the shell that every page loads.
        manualChunks: {
          monaco: ['monaco-editor', '@monaco-editor/react'],
          xterm: ['@xterm/xterm', '@xterm/addon-fit'],
          'react-vendor': ['react', 'react-dom', 'react-router-dom'],
        },
      },
    },
  },
  server: {
    port: 5173,
    host: '0.0.0.0',
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        ws: true,
      },

      '/ws': {
        target: 'ws://127.0.0.1:8000',
        ws: true,
        changeOrigin: true,
      },
      '/notebooks': {
        target: 'http://127.0.0.1:2718',
        changeOrigin: true,
        ws: true,
      },
      '/superset': {
        target: 'http://127.0.0.1:8088',
        changeOrigin: true,
      },
      '/dagster': {
        target: 'http://127.0.0.1:3000',
        changeOrigin: true,
        ws: true,
      },
      '/gitea': {
        target: 'http://127.0.0.1:3001',
        changeOrigin: true,
        // Gitea mounts every route at "/" and only prefixes the links it
        // generates with ROOT_URL's subpath, so strip /gitea before forwarding.
        // Bare "/gitea" (what the UI links to) becomes "/".
        rewrite: (path) => path.replace(/^\/gitea/, '') || '/',
      },
    },
  },
})
