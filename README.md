# runpod-jobrunner

`runpod-jobrunner` runs one bounded, recoverable job on RunPod. It records intent
before provider effects, executes a fixed five-phase remote protocol, verifies
declared artifacts, and deletes the worker before declaring the lifecycle closed.

The public interface is deliberately small:

```text
runpod-jobrunner check JOB_BUNDLE
runpod-jobrunner run JOB_BUNDLE --approve-max-usd USD
runpod-jobrunner status RUN_ID [--follow]
runpod-jobrunner stop RUN_ID
runpod-jobrunner recover RUN_ID
```

Several runs under one grant can reserve their full caps atomically:

```text
runpod-jobrunner run JOB_BUNDLE --approve-max-usd 47 \
  --budget-scope striatum-2026-07-31 --budget-total-usd 50
```

The project is under active bootstrap. See `docs/adr/` for the lifecycle contract.
