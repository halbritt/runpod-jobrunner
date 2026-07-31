#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: container/build-publish.sh [--push] IMAGE VERSION" >&2
}

publish=false
if [[ "${1:-}" == "--push" ]]; then
    publish=true
    shift
fi
if (( $# != 2 )); then
    usage
    exit 64
fi

readonly image="${1,,}"
readonly version="${2#v}"
if [[ ! "${image}" =~ ^[a-z0-9][a-z0-9._/-]*$ ]]; then
    echo "IMAGE is not a normalized registry path" >&2
    exit 64
fi
if [[ ! "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "VERSION must be an exact semantic version" >&2
    exit 64
fi
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
    echo "refusing to publish from a dirty build tree" >&2
    exit 65
fi

readonly source_revision="$(git rev-parse --verify HEAD)"
readonly short_revision="${source_revision:0:12}"
readonly source_date_epoch="$(git show -s --format=%ct "${source_revision}")"
readonly source_created="$(git show -s --format=%cI "${source_revision}")"
readonly repository_url="$(git config --get remote.origin.url || true)"
readonly project_version="$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')"
if [[ "${version}" != "${project_version}" ]]; then
    echo "VERSION ${version} differs from project version ${project_version}" >&2
    exit 65
fi

output=(--load)
if [[ "${publish}" == true ]]; then
    output=(--push --provenance=mode=max --sbom=true)
fi

docker buildx build \
    --platform linux/amd64 \
    --file container/Dockerfile \
    --build-arg "SOURCE_DATE_EPOCH=${source_date_epoch}" \
    --build-arg "SOURCE_REVISION=${source_revision}" \
    --build-arg "RUNNER_VERSION=${version}" \
    --label "org.opencontainers.image.created=${source_created}" \
    --label "org.opencontainers.image.revision=${source_revision}" \
    --label "org.opencontainers.image.source=${repository_url}" \
    --tag "${image}:${version}" \
    --tag "${image}:sha-${short_revision}" \
    "${output[@]}" \
    .

if [[ "${publish}" == true ]]; then
    docker buildx imagetools inspect "${image}:${version}"
else
    docker image inspect "${image}:${version}" --format '{{json .RepoDigests}}'
fi
