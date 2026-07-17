# Contributing

This repository is **mirrored** and read-only for contributors. The TypeScript client under `src/` is
assembled from the hiloop protobuf/OpenAPI contract plus its reviewed ergonomic layer, then
overwritten on every release.

To change the SDK, change its source in [`hiloopai/hiloop`](https://github.com/hiloopai/hiloop) and let
CI regenerate and re-mirror. Do not open PRs that edit `src/` here.
