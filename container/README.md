# No-op remote-runner image

This image is the generic integration target for the fixed-phase remote runner.
It starts the official RunPod `/start.sh` SSH bootstrap, waits at most 600
seconds for `request.json` and `status-token` on the mounted `/workspace`
storage, and then replaces its wrapper with `runpod-jobrunner-remote`. `tini`
remains PID 1. The runner publishes its release identity as `ready` and waits
for the separately hash-pinned launch token. The controller sends allow-listed
inputs only after it authenticates that identity. The runner serves read-only
status on port 8080; SSH remains on port 22.

The base is the official `runpod/base:1.1.0-ubuntu2404` image pinned to
`sha256:8fafffd0c67a48117dff09031ed16e79c0a83cc6996472f4b4dcb76343102a66`.
That digest was resolved for `linux/amd64` from Docker Hub on 2026-07-31 with:

```console
docker buildx imagetools inspect runpod/base:1.1.0-ubuntu2404
```

Build an exact version from a committed source revision:

```console
container/build-publish.sh ghcr.io/halbritt/runpod-jobrunner-noop 0.1.12
```

Publishing is an explicit operation. Authenticate the Docker client without
putting credentials in the build context, then use `--push`:

```console
container/build-publish.sh --push ghcr.io/halbritt/runpod-jobrunner-noop 0.1.12
```

`Dockerfile.dockerignore` allow-lists only the package metadata, Python source,
Dockerfile, entrypoint, and no-op phase command. The build context therefore
does not contain the repository metadata, developer configuration, or local
credential files.

The script also tags the source revision as `sha-<12 hex characters>` and, after
a push, prints the registry manifest digest. Put only that immutable digest form
in a job bundle. The release workflow accepts exact semantic versions and uses
the repository-scoped GitHub token only for registry login; it is never passed
as a build argument or copied into an image layer.
