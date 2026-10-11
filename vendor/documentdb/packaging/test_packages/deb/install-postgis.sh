#!/bin/bash
# Install PostGIS into the DEB test image: use the PGDG package when available,
# otherwise build from source and register a dependency-only package stub.
set -euo pipefail

PGVERSION="${1:?usage: install-postgis.sh <pg-major>}"
INSTALL_DEPENDENCIES_ROOT="${INSTALL_DEPENDENCIES_ROOT:-/tmp/install_setup}"

apt-get update
if apt-get install -y --no-install-recommends "postgresql-${PGVERSION}-postgis-3"; then
    exit 0
fi

echo "postgresql-${PGVERSION}-postgis-3 unavailable from PGDG; building postgis from source"

apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    autoconf \
    automake \
    libtool \
    "postgresql-server-dev-${PGVERSION}" \
    libproj-dev \
    libxml2-dev \
    libjson-c-dev \
    libgeos-dev \
    libgeos++-dev

export INSTALL_DEPENDENCIES_ROOT
PGVERSION="$PGVERSION" "$INSTALL_DEPENDENCIES_ROOT/install_setup_postgis.sh"

STUB_DIR=$(mktemp -d)
mkdir -p "$STUB_DIR/pkg/DEBIAN"
cat > "$STUB_DIR/pkg/DEBIAN/control" <<EOF
Package: postgresql-${PGVERSION}-postgis-3
Version: 0~local-source
Architecture: all
Maintainer: documentdb-ci <none@localhost>
Description: stub marking the from-source postgis build in this test image
EOF
dpkg-deb --build "$STUB_DIR/pkg" "$STUB_DIR/stub.deb"
dpkg -i "$STUB_DIR/stub.deb"
rm -rf "$STUB_DIR"
