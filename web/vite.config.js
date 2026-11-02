import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: './',
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://127.0.0.1:8091',
      '/v1': 'http://127.0.0.1:8091',
      '/events': 'http://127.0.0.1:8091',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
