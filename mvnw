#!/bin/sh
# Minimal Apache Maven Wrapper.
# Downloads the Maven distribution pinned in
# .mvn/wrapper/maven-wrapper.properties when no cached copy exists.
# (The Docker `builder` service uses the maven:3.9.9 image and does not
# need this script — it's here for local builds only.)
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
PROPS="$DIR/.mvn/wrapper/maven-wrapper.properties"

if [ ! -f "$PROPS" ]; then
  echo "Cannot find $PROPS" >&2
  exit 1
fi

DIST_URL="$(sed -n 's/^distributionUrl=//p' "$PROPS" | tr -d '\r')"
MVN_VERSION="$(basename "$DIST_URL" | sed 's/^apache-maven-//; s/-bin.zip$//')"
WRAPPER_HOME="${MAVEN_USER_HOME:-$HOME/.m2}/wrapper/dists/apache-maven-$MVN_VERSION"
MVN_BIN="$WRAPPER_HOME/bin/mvn"

if [ ! -x "$MVN_BIN" ]; then
  echo "Downloading Apache Maven $MVN_VERSION ..."
  rm -rf "$WRAPPER_HOME" "$WRAPPER_HOME.tmp"
  mkdir -p "$WRAPPER_HOME.tmp"
  ZIP="$WRAPPER_HOME.tmp/maven.zip"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$ZIP" "$DIST_URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$ZIP" "$DIST_URL"
  else
    echo "Need curl or wget to download Maven" >&2
    exit 1
  fi
  unzip -q "$ZIP" -d "$WRAPPER_HOME.tmp"
  mkdir -p "$(dirname "$WRAPPER_HOME")"
  mv "$WRAPPER_HOME.tmp/apache-maven-$MVN_VERSION" "$WRAPPER_HOME"
  rm -rf "$WRAPPER_HOME.tmp"
fi

exec "$MVN_BIN" "$@"
