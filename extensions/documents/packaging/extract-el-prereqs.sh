#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# extract-el-prereqs.sh — print the RHEL/EL prerequisite commands, extracted
# from the ONE authoritative copy in packaging/README.md.
#
# packaging/README.md fences that copy with <!-- BEGIN/END:el-prereqs -->. Every
# other place that shows a user these commands calls this script instead of
# restating them, so the copies cannot drift:
#
#   - packaging/test_packages/test-documented-prereqs.sh   (executes them)
#   - .github/workflows/documentdb_release.yml             (release body)
#   - .github/workflows/build_all_packages.yml             (bundle job summary)
#
# Three copies cannot be generated -- the two RPM %description blocks are baked
# in at package build time, and the gateway design doc is prose-of-record. For
# those, --check-copies asserts they still carry every documented prerequisite.
#
# Usage:
#   ./packaging/extract-el-prereqs.sh [--el 9|8] [--arch x86_64|aarch64] [--no-sudo]
#   ./packaging/extract-el-prereqs.sh --check-copies

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README="${REPO_ROOT}/packaging/README.md"

EL_MAJOR=9
ARCH=x86_64
STRIP_SUDO=0
CHECK_COPIES=0

die() { echo "[el-prereqs] $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --el)          EL_MAJOR="$2"; shift 2 ;;
        --arch)        ARCH="$2"; shift 2 ;;
        --no-sudo)     STRIP_SUDO=1; shift ;;
        --check-copies) CHECK_COPIES=1; shift ;;
        -h|--help)     sed -n '1,25p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *)             die "unknown argument: $1" ;;
    esac
done

[[ "${EL_MAJOR}" == "9" || "${EL_MAJOR}" == "8" ]] || die "--el must be 9 or 8 (got ${EL_MAJOR})"
[[ "${ARCH}" == "x86_64" || "${ARCH}" == "aarch64" ]] || die "--arch must be x86_64 or aarch64 (got ${ARCH})"
[[ -r "${README}" ]] || die "cannot read ${README}"

# EL8 calls the CodeReady Builder repo "powertools"; EL9 renamed it to "crb".
CRB_REPO=crb
[[ "${EL_MAJOR}" == "8" ]] && CRB_REPO=powertools

# Exactly one BEGIN and one END. Without this a stray second marker (or a lost
# END) silently re-opens the block and concatenates unrelated README lines into
# the command list — which is then published to users AND executed in CI.
begin_count=$(grep -c 'BEGIN:el-prereqs' "${README}")
end_count=$(grep -c 'END:el-prereqs' "${README}")
(( begin_count == 1 )) || die "expected exactly 1 BEGIN:el-prereqs marker in ${README}, found ${begin_count}"
(( end_count == 1 ))   || die "expected exactly 1 END:el-prereqs marker in ${README}, found ${end_count}"

# Take the ```bash fence between the markers and strip the blockquote prefix.
# The fence and its body must keep the "> " prefix — the block lives inside a
# Markdown blockquote and un-quoting it is an easy, invisible way to break this.
CMDS="$(awk '
    /BEGIN:el-prereqs/      { inblock = 1; next }
    /END:el-prereqs/        { inblock = 0; next }
    inblock && /^> ```/     { infence = !infence; next }
    inblock && infence      { sub(/^> ?/, ""); print }
' "${README}")"

if [[ -z "${CMDS}" ]]; then
    # Distinguish the two ways this happens, because the fix differs and a
    # wrong diagnosis sends the reader hunting the wrong thing.
    if grep -qE '^> ```' <(awk '/BEGIN:el-prereqs/{f=1} /END:el-prereqs/{f=0} f' "${README}"); then
        die "the fenced block between the el-prereqs markers yielded no commands in ${README} — check that BOTH fence lines and every command line keep the leading '> ' (the block lives inside a Markdown blockquote)"
    fi
    die "found the el-prereqs markers in ${README} but no blockquoted \`\`\` fence between them — the fence and every command line must keep the leading '> ' (the block lives inside a Markdown blockquote)"
fi

# Guard the extraction: a silent mis-parse that yielded prose would otherwise
# be published to users (or "executed" and pass trivially).
while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    # `#` lines are allowed on purpose: they are inert when the block is
    # executed verbatim in CI, but carry the guidance that cannot be a dnf
    # command — notably subscribed RHEL, which has no `crb` repo id and needs
    # `subscription-manager repos --enable codeready-builder-...` instead.
    [[ "${line}" =~ ^# ]] && continue
    [[ "${line}" =~ ^sudo[[:space:]]+dnf[[:space:]] ]] || \
        die "extracted a line that is neither a comment nor a dnf command from the README block: '${line}'"
done <<< "${CMDS}"

# One tripwire PER documented command. Two is not enough: a deletion is
# invisible to --check-copies (the other copies stay a superset, so presence
# still holds), and the install test does not backstop every line either —
# `module disable postgresql` can be dropped with DOC-SUFFICIENT staying green,
# because the RPM Requires the versioned `postgresql%{N}`/`-server` names that
# the AppStream module never provides or shadows.
require_cmd() {
    grep -q -- "$1" <<< "${CMDS}" || die "the README prerequisite block no longer contains '$1' — $2"
}
require_cmd "dnf-plugins-core"   "dnf config-manager comes from it; without it the next line fails on a minimal host"
require_cmd "pgdg-redhat-repo"   "the PGDG repository is where pgvector_N, pg_cron_N and postgis36_N come from"
require_cmd "epel-release"       "PGDG's postgis36_N needs hdf5 and xerces-c from EPEL"
require_cmd "--set-enabled crb"  "that is exactly the regression this tooling exists to catch (GitHub issue #75)"
require_cmd "module disable postgresql" "the AppStream postgresql module otherwise shadows the PGDG packages for anyone installing unversioned names"

# --check-copies: verify (rather than generate) the copies that cannot be
# generated -- the two RPM %description blocks and the gateway design doc.
if (( CHECK_COPIES )); then
    # Whitespace-normalise both sides. `sed -E` (not BRE `\+`) because BSD sed
    # on macOS treats `\+` as a literal plus and silently normalises nothing.
    # Also fold the EL major and arch in the PGDG URL to placeholders, so a
    # copy that is arch-correct (EL-9-aarch64, or an rpm-expanded EL-9-%{_arch})
    # compares equal instead of being reported as drift. The gate must not make
    # the RIGHT edit fail.
    normalise() {
        sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//
                s#/EL-[0-9]+-[A-Za-z0-9_%{}]+/#/EL-N-ARCH/#g'
    }
    status=0
    for spec in "${REPO_ROOT}/packaging/rpm/spec/documentdb.spec" \
                "${REPO_ROOT}/packaging/rpm/spec/documentdb-local-meta.spec" \
                "${REPO_ROOT}/packaging/gateway/packaging-design.md"; do
        [[ -r "${spec}" ]] || { echo "[el-prereqs] cannot read ${spec}" >&2; status=1; continue; }
        # Scope to what actually SHIPS. RPM comment lines never reach the
        # built package, so grepping the whole spec would report OK for
        # guidance that `dnf info` will not show. %description is the only
        # part users can read before installing.
        case "${spec}" in
            *.spec) spec_body="$(awk '/^%description/{f=1;next} /^%[a-zA-Z]+/{f=0} f' "${spec}")" ;;
            *)      spec_body="$(cat "${spec}")" ;;
        esac
        spec_norm="$(normalise <<< "${spec_body}")"
        missing=()
        # PRESENCE, not set equality or ordering: a copy may legitimately carry
        # other dnf lines (documentdb-local-meta.spec documents `dnf remove`) or
        # list these in a different order (packaging-design.md does). The
        # invariant is only that every documented prerequisite still appears.
        while IFS= read -r cmd; do
            [[ -z "${cmd}" ]] && continue
            [[ "${cmd}" =~ ^# ]] && continue   # comments are README-only guidance
            cmd="$(normalise <<< "${cmd}")"
            grep -qF -- "${cmd}" <<< "${spec_norm}" || missing+=("${cmd}")
        done <<< "${CMDS}"

        if (( ${#missing[@]} == 0 )); then
            echo "[el-prereqs] OK  $(basename "${spec}") carries every documented prerequisite"
        else
            echo "[el-prereqs] DRIFT  $(basename "${spec}") is missing prerequisite lines that packaging/README.md documents:" >&2
            printf '    %s\n' "${missing[@]}" >&2
            status=1
        fi
    done
    exit "${status}"
fi

# Apply exactly the substitutions the README documents for other EL majors and
# architectures.
# `sed -E`, not BRE: BSD sed (macOS) treats `\+` as a literal plus, so the
# BRE spelling silently rewrote nothing and emitted an EL-9 URL for an EL-8 run.
OUT="$(sed -E -e "s#/EL-[0-9]+-#/EL-${EL_MAJOR}-#" \
              -e "s#/EL-${EL_MAJOR}-[A-Za-z0-9_]+/#/EL-${EL_MAJOR}-${ARCH}/#" \
              -e "s/--set-enabled crb/--set-enabled ${CRB_REPO}/" \
              <<< "${CMDS}")"

# Fail CLOSED if a rewrite did not land. These seds are pinned to today's URL
# shape; a routine upstream change (EL-10, a new reporpms path) would otherwise
# emit an internally contradictory block — an EL-9 URL beside --set-enabled
# powertools — that CI then runs and blames on the docs.
grep -q "/EL-${EL_MAJOR}-${ARCH}/" <<< "${OUT}" || \
    die "the EL-major/arch rewrite did not match the PGDG URL in the README block (expected /EL-${EL_MAJOR}-${ARCH}/). The URL shape changed — update the substitutions in $(basename "${BASH_SOURCE[0]}")."
grep -q -- "--set-enabled ${CRB_REPO}" <<< "${OUT}" || \
    die "the CRB repo-name rewrite did not land (expected --set-enabled ${CRB_REPO})"

if (( STRIP_SUDO )); then sed 's/^sudo //' <<< "${OUT}"; else printf '%s\n' "${OUT}"; fi
