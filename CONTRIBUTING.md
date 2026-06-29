# Contributing

This repository is **generated** and read-only for contributors. The TypeScript client under `src/` is
produced from the hiloop protobuf/OpenAPI contract and overwritten on every release.

To change the SDK, change the contract in [`hiloopai/hiloop`](https://github.com/hiloopai/hiloop)
(`proto/`) and let CI regenerate and re-mirror. Do not open PRs that edit `src/` here.
