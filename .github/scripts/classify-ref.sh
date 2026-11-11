#!/usr/bin/env bash
# Says what kind of release, if any, the ref being built is. Prints two
# `name=value` lines for $GITHUB_OUTPUT.
#
#   final=true       the ref is a tag vX.Y.Z and nothing after it. Only this
#                    moves `latest` and the X.Y / X image tags.
#   prerelease=true  the ref is a tag vX.Y.Z-anything, such as v0.2.0-rc.1.
#                    It is published under its own tag only.
#
# Anything else, a branch included, is neither: it gets an image and no
# release.
set -euo pipefail

final=false
prerelease=false
if [ "${GITHUB_REF_TYPE:-}" = "tag" ]; then
  if [[ "${GITHUB_REF_NAME:-}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    final=true
  elif [[ "${GITHUB_REF_NAME:-}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-.+$ ]]; then
    prerelease=true
  fi
fi

echo "final=${final}"
echo "prerelease=${prerelease}"
