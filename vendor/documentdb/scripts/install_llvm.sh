#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command exits with a non-zero status
set -e

LLVM_VERSION=20

# apt.llvm.org lags new Ubuntu releases; when the running codename has no
# upstream repo yet, install the same LLVM major from the distro archive.
# A failed network request is not evidence that the repository is missing.
CODENAME=$(. /etc/os-release && echo "${VERSION_CODENAME:-}")
REPOSITORY_URL="https://apt.llvm.org/$CODENAME/dists/llvm-toolchain-$CODENAME-$LLVM_VERSION/Release"
HTTP_STATUS=$(curl --location --silent --show-error --retry 3 --retry-connrefused \
    --connect-timeout 15 --max-time 60 --output /dev/null \
    --write-out '%{http_code}' "$REPOSITORY_URL")
if [ "$HTTP_STATUS" = "200" ]; then
    curl --fail --location --show-error --retry 3 --retry-connrefused --connect-timeout 15 \
        --max-time 120 --output llvm.sh https://apt.llvm.org/llvm.sh
    chmod +x ./llvm.sh
    sudo ./llvm.sh $LLVM_VERSION all
elif [ "$HTTP_STATUS" = "404" ]; then
    sudo apt-get update
    sudo apt-get install -y clang-$LLVM_VERSION clang-tools-$LLVM_VERSION \
        llvm-$LLVM_VERSION lld-$LLVM_VERSION libclang-$LLVM_VERSION-dev
else
    echo "Cannot check LLVM repository $REPOSITORY_URL: HTTP $HTTP_STATUS" >&2
    exit 1
fi
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/clang-cl /usr/bin/clang-cl
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/llvm-lib /usr/bin/llvm-lib
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/lld-link /usr/bin/lld-link
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/llvm-ml /usr/bin/llvm-ml
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/ld.lld /usr/bin/ld.lld
sudo ln -s /usr/lib/llvm-$LLVM_VERSION/bin/clang /usr/bin/clang