---
"txob": patch
---

Fix CommonJS consumers: each export now gives `require` its own CommonJS type declarations, the build no longer copies a `"type": "module"` package.json into `dist/`, and `engines.node` is now `^20.19.0 || >=22.12.0` (the versions that can `require()` the ESM-only `p-limit` and `p-queue`).
