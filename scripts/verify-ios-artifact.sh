#!/bin/sh
set -eu

bin=${1:?usage: verify-ios-artifact.sh PATH_TO_BINARY}
min_ios=${MIN_IOS_VERSION:-15.0}

file "$bin" | tee verify-file.txt
file "$bin" | grep -q 'Mach-O 64-bit executable arm64'

xcrun vtool -show-build "$bin" | tee verify-build.txt
grep -Eq 'platform IOS|platform 2' verify-build.txt
grep -Eq "minos[[:space:]]+$min_ios([[:space:]]|$)" verify-build.txt

xcrun otool -L "$bin" | tee verify-libraries.txt
if grep -E '/(Users|home|opt/homebrew|usr/local)/' verify-libraries.txt; then
  echo 'unexpected build-host path in load commands' >&2
  exit 1
fi
xcrun otool -l "$bin" | tee verify-load-commands.txt
xcrun codesign -dvvv --entitlements :- "$bin" >verify-signature.txt 2>&1
xcrun codesign --verify --verbose=4 "$bin"

# Go paths and source checkout paths must not leak into the stripped executable.
if strings "$bin" | grep -E '/Users/runner/work/|/home/runner/|/workspace/' >/dev/null; then
  echo 'build-host absolute path found in binary' >&2
  exit 1
fi
