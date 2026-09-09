import { defineConfig } from "vite";

export default defineConfig({
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8911",
    },
  },
  build: {
    rollupOptions: {
      output: {
        entryFileNames: "assets/app.js",
        assetFileNames: "assets/app.[ext]",
      },
    },
  },
});
