#!/usr/bin/env bash
# Pre-release check: is this repo safe to hand to the broader research community
# who use Oracle Health real-world data?  Looks for (1) PHI / data files,
# (2) references to a specific health system's identity or assets,
# (3) credentials and personal absolute paths.
#
# Usage: scripts/check-public-safe.sh [--history]
#   --history  also scan every commit's diff (what a public push would expose)
#
# Exit 0 = clean, 1 = findings.  Identifier patterns are base64-encoded so this
# file does not itself trip the compliance-scan workflow (same convention).
# Matched text is never printed for PHI-class hits, only file:line.
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2

dec() { printf '%s' "$1" | base64 --decode; }
ORG_RE="$(dec 'aXVbIF8tXT9oZWFsdGg=')|$(dec 'XGJpdWg=')|$(dec 'ZGV2XC5henVyZVwuY29t')|$(dec 'cndkXC5vcmc=')"
SECRET_RE='(password|passwd|secret|api[_-]?key|passphrase)[[:space:]]*[:=][[:space:]]*["'"'"']?[A-Za-z0-9/+_.-]{6,}|AKIA[0-9A-Z]{16}|BEGIN (RSA|OPENSSH|EC|PRIVATE)'
PATH_RE='/home/[a-z][a-z0-9_]+/|/Users/[A-Za-z][A-Za-z0-9_]+/|s3://[a-z0-9-]+-[a-z0-9-]+'
PHI_RE='\bSSN[: ]+[0-9]|\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b|\bMRN[: #]*[0-9]{4,}|\b\(?[0-9]{3}\)?[ .-][0-9]{3}[ .-][0-9]{4}\b|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(org|edu|com|net)'
DATA_EXT='\.(csv|tsv|parquet|xlsx?|pkl|sqlite|db|ipynb|html|log)$'

fail=0
report() { echo; echo "== $1"; }

files() { git ls-files; git ls-files --others --exclude-standard; }
files=$(files | grep -v '^scripts/check-public-safe.sh$' | sort -u)

report "Organisation-specific identifiers (name, hostnames, user ids, bucket names)"
hits=$(echo "$files" | xargs -d '\n' grep -I -n -i -E "$ORG_RE" 2>/dev/null | cut -c1-140)
[ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"

report "Credentials / secrets"
hits=$(echo "$files" | xargs -d '\n' grep -I -n -i -E "$SECRET_RE" 2>/dev/null | cut -c1-140)
[ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"

report "Personal absolute paths / storage locations"
hits=$(echo "$files" | xargs -d '\n' grep -I -n -E "$PATH_RE" 2>/dev/null | cut -c1-140)
[ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"

report "PHI-shaped values (SSN, MRN+digits, phone, email) — locations only"
hits=$(echo "$files" | xargs -d '\n' grep -I -n -E "$PHI_RE" 2>/dev/null | grep -v -E 'noreply|@example\.' | cut -d: -f1,2)
[ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"

report "Data-like files (review each: schema/example only, or real rows?)"
hits=$(echo "$files" | grep -i -E "$DATA_EXT")
[ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"

if [ "${1:-}" = "--history" ]; then
  report "History: identifiers in any commit (commit:file)"
  hits=$(git log --all -i -E --grep='' --format= -G"$ORG_RE" --name-only 2>/dev/null | sort -u | head -50)
  [ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"
  report "History: data-like files ever committed"
  hits=$(git log --all --name-only --format= | sort -u | grep -i -E "$DATA_EXT")
  [ -n "$hits" ] && { echo "$hits"; fail=1; } || echo "none"
  report "History: author identities"
  git log --all --format='%an <%ae>' | sort | uniq -c
fi

echo
[ "$fail" -eq 0 ] && echo "PASS: nothing found" || echo "FAIL: review findings above"
exit "$fail"
