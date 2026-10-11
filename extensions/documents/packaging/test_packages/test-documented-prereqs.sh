#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# test-documented-prereqs.sh — prove the DOCUMENTED RHEL prerequisite list is
# SUFFICIENT to install the DocumentDB extension RPM on a stock EL host.
#
# Why this exists (GitHub issue #75). The extension RPM hard-Requires
# postgis36_N; PGDG's postgis36_N pulls gdal*-libs; gdal*-libs needs
# libqhull_r.so.7, which ships ONLY in EL9's CRB (EL8: powertools) repository —
# disabled by default on Rocky/RHEL/Alma. With the then-documented PGDG + EPEL
# prerequisites alone, `dnf install` died in dependency resolution with ~20
# near-identical GDAL candidate lines that never named qhull's repository, so
# the Tier-1 RHEL platform was uninstallable by the book.
#
# Every other RHEL test image enables CRB in its OWN Dockerfile, on a line
# separate from the documented block. That makes them structurally incapable of
# catching this class of bug: the image is always correct no matter what the
# docs say. This suite closes that gap from the other side — it takes the
# prerequisite commands from packaging/README.md (via extract-el-prereqs.sh)
# and runs them VERBATIM on a stock base image, then installs the real .rpm.
# The docs are the input, so docs and dependency tree cannot drift apart
# silently.
#
# A NEGATIVE CONTROL runs the same block with the CRB line removed and requires
# the install to FAIL. Without it this suite would quietly become a no-op the
# day a base image starts shipping CRB enabled — passing while proving nothing.
#
# Scope: this installs the extension RPM (the package in the issue #75 repro).
# That transitively exercises most of the documented repo set — PGDG for the
# extension packages, CRB for libqhull_r, EPEL for postgis36_N's hdf5 and
# xerces-c. It does NOT backstop every line: `module disable postgresql` can be
# dropped with this suite staying green, because the RPM Requires the versioned
# postgresql%{N} names the AppStream module never shadows. Per-line deletions
# are caught by the tripwires in extract-el-prereqs.sh instead — that is where
# the coverage for a shrinking block lives, not here.
#
# Usage:
#   ./packaging/test_packages/test-documented-prereqs.sh [--pg N] [--el 9|8]
#                                [--packages-dir DIR] [--image IMG] [--skip-negative]
#
# Runs on the HOST (needs docker), not inside a test container.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXTRACT="${REPO_ROOT}/packaging/extract-el-prereqs.sh"

PG_MAJOR=18
EL_MAJOR=9
PACKAGES_DIR="${REPO_ROOT}/packaging"
BASE_IMAGE=""
RUN_NEGATIVE=1

PASS_COUNT=0
FAIL_COUNT=0
FAILED_IDS=()
TMP_FILES=()
# Pre-set: run_install can bail before assigning it (mktemp failure), and
# `set -u` would then kill the run before the summary ever prints.
LAST_LOG=""
cleanup() { (( ${#TMP_FILES[@]} )) && rm -f "${TMP_FILES[@]}"; }
trap cleanup EXIT

log()  { echo -e "\033[1;36m[prereqs]\033[0m $*"; }
pass() { PASS_COUNT=$((PASS_COUNT + 1)); echo -e "\033[1;32mPASS\033[0m $*"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); FAILED_IDS+=("$1"); echo -e "\033[1;31mFAIL\033[0m $*"; }
die()  { echo -e "\033[1;31m[prereqs] $*\033[0m" >&2; exit 2; }
tail_log() { [[ -r "${LAST_LOG}" ]] && tail -n 25 "${LAST_LOG}"; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pg)            PG_MAJOR="$2"; shift 2 ;;
        --el)            EL_MAJOR="$2"; shift 2 ;;
        --packages-dir)  PACKAGES_DIR="$2"; shift 2 ;;
        --image)         BASE_IMAGE="$2"; shift 2 ;;
        --skip-negative) RUN_NEGATIVE=0; shift ;;
        -h|--help)       sed -n '1,40p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *)               die "unknown argument: $1" ;;
    esac
done

[[ "${EL_MAJOR}" == "9" || "${EL_MAJOR}" == "8" ]] || die "--el must be 9 or 8 (got ${EL_MAJOR})"
command -v docker >/dev/null 2>&1 || die "docker is required"
[[ -x "${EXTRACT}" ]] || die "cannot execute ${EXTRACT}"

CRB_REPO=crb
[[ "${EL_MAJOR}" == "8" ]] && CRB_REPO=powertools
[[ -n "${BASE_IMAGE}" ]] || BASE_IMAGE="rockylinux/rockylinux:${EL_MAJOR}"

# ── 1. Locate the extension RPM ────────────────────────────────────────────
# Done BEFORE extracting the commands: the package's architecture selects the
# PGDG repo RPM the documented block installs (EL-N-x86_64 vs EL-N-aarch64).
# Exclude debug siblings explicitly. They are suppressed today only by
# `%define debug_package %{nil}` in the spec; if that is ever dropped, a
# dependency-free ...-debuginfo-....rpm would match this glob with a near
# identical mtime, DOC-SUFFICIENT would pass vacuously and NEGATIVE-CONTROL
# would report "install SUCCEEDED without crb".
RPM_PATH="$(find "${PACKAGES_DIR}" -maxdepth 1 \
    -name "rhel${EL_MAJOR}-postgresql${PG_MAJOR}-documentdb-*.rpm" \
    ! -name '*debuginfo*' ! -name '*debugsource*' ! -name '*dbgsym*' \
    -printf '%T@ %p\n' 2>/dev/null | LC_ALL=C sort -rn | head -n 1 | cut -d' ' -f2-)"

# BSD find (macOS) has no -printf; fall back to a plain newest-by-mtime pick.
if [[ -z "${RPM_PATH}" ]]; then
    RPM_PATH="$(ls -t "${PACKAGES_DIR}"/rhel${EL_MAJOR}-postgresql${PG_MAJOR}-documentdb-*.rpm 2>/dev/null \
                | grep -vE 'debuginfo|debugsource|dbgsym' | head -n 1)"
fi

# Absolutise BEFORE docker sees it: a relative path (e.g. --packages-dir
# packaging, the natural invocation from the repo root) is interpreted by
# docker as a NAMED VOLUME, which fails with "invalid characters for a local
# volume name" and gets misreported as an infrastructure failure.
if [[ -n "${RPM_PATH}" && "${RPM_PATH}" != /* ]]; then
    RPM_PATH="$(cd "$(dirname "${RPM_PATH}")" && pwd)/$(basename "${RPM_PATH}")"
fi

[[ -n "${RPM_PATH}" && -r "${RPM_PATH}" ]] || \
    die "no rhel${EL_MAJOR}-postgresql${PG_MAJOR}-documentdb-*.rpm in ${PACKAGES_DIR} — build it first: ./packaging/build_packages.sh --os rhel${EL_MAJOR} --pg ${PG_MAJOR}"

# Match the container platform AND the documented PGDG URL to the package arch.
case "${RPM_PATH}" in
    *.x86_64.rpm)  RPM_ARCH=x86_64;  PLATFORM=linux/amd64 ;;
    *.aarch64.rpm) RPM_ARCH=aarch64; PLATFORM=linux/arm64 ;;
    *)             die "cannot determine arch from $(basename "${RPM_PATH}")" ;;
esac

# ── 2. Take the commands from packaging/README.md ──────────────────────────
# extract-el-prereqs.sh owns the parsing and the doc-regression tripwires
# (non-dnf lines, a dropped CRB line, a dropped epel-release line).
PREREQ_CMDS="$("${EXTRACT}" --el "${EL_MAJOR}" --arch "${RPM_ARCH}" --no-sudo)" || \
    die "could not extract the prerequisite block from packaging/README.md (see above)"

log "README block (EL${EL_MAJOR}, ${RPM_ARCH}):"
sed 's/^/    /' <<< "${PREREQ_CMDS}"
log "package : $(basename "${RPM_PATH}")"
log "image   : ${BASE_IMAGE} (${PLATFORM})"

# ── 3. Run one install attempt in a stock container ────────────────────────
# A dnf exit of 1 means "install did not happen" and says nothing about WHY:
# an unmet dependency and a five-minute PGDG mirror blip look identical. Only a
# depsolve signature in the log licenses the claim that the docs are wrong, so
# classify before reporting and retry the transient case rather than filing it
# as a documentation bug (the nightly opens an issue on failure).
RESOLUTION_SIGNATURE='nothing provides|but none of the providers can be installed|cannot install the best candidate|conflicting requests|Depsolve Error'

# $1 = variant label, $2 = prerequisite commands. Sets LAST_LOG. Returns the
# dnf exit status, or 90 if the container never reached the install.
run_install_once() {
    local label="$1" cmds="$2" runner
    # X's must be LAST in the template: BSD mktemp (macOS, where maintainers
    # run this by hand) treats a trailing ".sh" as literal and then collides
    # with itself on the second call.
    runner="$(mktemp "${TMPDIR:-/tmp}/prereq-${label}-XXXXXX")" || return 90
    TMP_FILES+=("${runner}" "${runner}.log")
    {
        echo 'set -x'
        # Stop at the FIRST failing prerequisite command. Without this, a 5xx
        # or DNS failure on the single-origin PGDG repo RPM just continues, and
        # the final install then dies with `nothing provides pg_cron_N` — a
        # textbook depsolve signature, which the classifier below deliberately
        # does NOT retry and would report as "the docs are insufficient".
        echo 'set -e'
        echo "trap 'st=\$?; echo \"PREREQ_EXIT=\${st}\"; exit \${st}' ERR"
        echo "${cmds}"
        echo 'trap - ERR'
        echo 'set +e'
        echo 'set +x'
        echo 'echo "===== dnf install (documented path) ====="'
        echo 'dnf install -y /tmp/documentdb.rpm'
        echo 'echo "INSTALL_EXIT=$?"'
    } > "${runner}"

    docker run --rm --platform "${PLATFORM}" \
        -v "${runner}:/runner.sh:ro" \
        -v "${RPM_PATH}:/tmp/documentdb.rpm:ro" \
        "${BASE_IMAGE}" bash /runner.sh 2>&1 | tee "${runner}.log"

    local status
    LAST_LOG="${runner}.log"
    # 91 = a prerequisite command itself failed. Distinct from 90 so the report
    # can name what actually broke instead of blaming the dependency tree.
    #
    # ANCHOR both greps at column 0. `set -x` echoes the trap DEFINITION, whose
    # text contains the literal "PREREQ_EXIT=", so an unanchored match fired on
    # every single run and reported a prerequisite failure that never happened.
    # Real markers are printed by echo at column 0; xtrace lines start with "+".
    grep -qE '^PREREQ_EXIT=' "${runner}.log" 2>/dev/null && return 91
    status="$(grep -oE '^INSTALL_EXIT=[0-9]+' "${runner}.log" | tail -n 1 | cut -d= -f2)"
    [[ -n "${status}" ]] || return 90   # container never reached the install
    return "${status}"
}

# Same, but retries once when the failure has no depsolve signature — i.e. when
# it looks like a mirror/registry blip rather than a statement about the
# dependency tree. A genuine depsolve failure is NOT retried; it is the answer.
run_install() {
    local label="$1" cmds="$2" status
    run_install_once "${label}" "${cmds}"
    status=$?
    if (( status != 0 )) && { (( status == 91 )) || ! grep -qE "${RESOLUTION_SIGNATURE}" "${LAST_LOG}" 2>/dev/null; }; then
        log "no depsolve signature in the failure — retrying once in case it was a transient mirror/registry error"
        run_install_once "${label}-retry" "${cmds}"
        status=$?
    fi
    return "${status}"
}

# Report a failed attempt, distinguishing "the docs are wrong" from "the
# network was". $1 = id, $2 = status, $3 = message used only for a real
# depsolve failure.
report_failure() {
    local id="$1" status="$2" doc_msg="$3"
    if (( status == 90 )); then
        fail "${id}" "the container never reached the install step (docker or network failure) — this run proves nothing either way"
    elif (( status == 91 )); then
        fail "${id}" "a prerequisite command from the README block failed before the install (see the failing command above) — mirror/registry trouble or a bad command in the block, NOT evidence about the dependency tree"
    elif grep -qE "${RESOLUTION_SIGNATURE}" "${LAST_LOG}" 2>/dev/null; then
        fail "${id}" "${doc_msg}"
    else
        fail "${id}" "install failed with no depsolve signature, twice — treat as infrastructure (mirror/registry), NOT as a documentation bug"
    fi
    echo "--- tail of the failing run ---"
    tail_log
}

# DOC-SUFFICIENT — the documented block must be enough, start to finish.
log "=== DOC-SUFFICIENT: documented prerequisites -> dnf install ==="
run_install sufficient "${PREREQ_CMDS}"
pos_status=$?
if (( pos_status == 0 )); then
    pass "DOC-SUFFICIENT: the documented prerequisites install postgresql${PG_MAJOR}-documentdb on stock EL${EL_MAJOR}/${RPM_ARCH}"
else
    report_failure "DOC-SUFFICIENT" "${pos_status}" \
        "the documented prerequisite block does NOT install the RPM on a stock EL${EL_MAJOR} host — the docs are insufficient (this is the issue #75 class of bug)"
fi

# NEGATIVE-CONTROL — drop the CRB line; the install must fail on qhull. This is
# what keeps DOC-SUFFICIENT honest.
if (( RUN_NEGATIVE )); then
    log "=== NEGATIVE-CONTROL: same block WITHOUT ${CRB_REPO} -> install must fail ==="
    NO_CRB_CMDS="$(grep -v -- "--set-enabled ${CRB_REPO}" <<< "${PREREQ_CMDS}")"
    run_install negative "${NO_CRB_CMDS}"
    neg_status=$?
    if (( neg_status == 0 )); then
        fail "NEGATIVE-CONTROL" "install SUCCEEDED without ${CRB_REPO} — the base image or PGDG changed and DOC-SUFFICIENT no longer proves anything; re-check whether CRB is still required before trusting this suite"
    elif (( neg_status == 90 )); then
        fail "NEGATIVE-CONTROL" "the container never reached the install step (docker or network failure) — this run proves nothing either way"
        tail_log
    elif (( neg_status == 91 )); then
        fail "NEGATIVE-CONTROL" "a prerequisite command failed before the install — this run proves nothing either way"
        tail_log
    elif ! grep -qE "${RESOLUTION_SIGNATURE}" "${LAST_LOG}" 2>/dev/null; then
        # MUST precede the libqhull check. A download-phase failure prints the
        # resolved transaction table, which itself lists libqhull_r from crb —
        # so matching libqhull_r first would certify a non-depsolve failure as
        # "issue #75 reproduces" and vouch for a suite that proved nothing.
        fail "NEGATIVE-CONTROL" "install failed with no depsolve signature, twice — treat as infrastructure (mirror/registry), NOT as a documentation bug"
        tail_log
    elif grep -q "libqhull_r" "${LAST_LOG}" 2>/dev/null; then
        pass "NEGATIVE-CONTROL: without ${CRB_REPO} the install still fails on libqhull_r (issue #75 reproduces)"
    else
        # It failed to resolve, but not on qhull. The suite is still meaningful,
        # yet the troubleshooting entry in packaging/README.md is now keyed on
        # the wrong error string.
        fail "NEGATIVE-CONTROL" "install failed to resolve without ${CRB_REPO} but NOT on libqhull_r — update the troubleshooting entry in packaging/README.md to the current error"
        tail_log
    fi
fi

echo
echo "──────────────────────────────────────────────"
echo "  passed: ${PASS_COUNT}   failed: ${FAIL_COUNT}"
(( FAIL_COUNT )) && echo "  failed: ${FAILED_IDS[*]}"
echo "──────────────────────────────────────────────"
(( FAIL_COUNT == 0 ))
