import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import federation from '@originjs/vite-plugin-federation'

export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: 'FFprobeMediaInfoPersistence',
      filename: 'remoteEntry.js',
      exposes: {
        './Page': './src/Page.vue',
        './Config': './src/Config.vue',
      },
      shared: {
        vue: { requiredVersion: false, generate: false },
      },
      format: 'esm',
    }),
  ],
  build: {
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
    outDir: '../dist',
    emptyOutDir: true,
  },
})
