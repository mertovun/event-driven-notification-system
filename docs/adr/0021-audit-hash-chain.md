# ADR-0021: Audit log hash chain + content-integrity verifier

## Status

Accepted (2026-05-17).

## Context

Admin actions (DLQ replay, DLQ purge, API-key revoke) write to
`admin_audit`. Without an integrity check, an attacker (or a buggy admin
flow) can alter rows after the fact and an operator inspecting the table
has no signal that anything is wrong. The brief did not require an audit
chain, but the `admin_audit` table already exists and a tamper-evidence
layer is small and well-bounded if implemented correctly.

Two well-known failure modes for naive chain designs:

1. **Concurrent-insert race.** Two admin actions in two TXs both read
   the same `last_hash` from the previous row, both compute their
   `row_hash` chained off it, both commit. The second row's `prev_hash`
   matches the first row's *`prev_hash`*, not the first row's
   *`row_hash`* — a permanent broken link with no actual tampering.
2. **Content tampering without rehash.** A verifier that only checks
   `prev_hash == LAG(row_hash)` passes when an attacker edits a column
   in place and leaves `row_hash` untouched. The chain proves nobody
   deleted or reordered rows; it does **not** prove individual rows
   are intact unless the verifier recomputes the digest from the
   row's contents.

Both shipped in earlier passes and both had to be fixed.

## Decision

A three-migration chain ending at content-integrity verification:

- **Migration 0009** — `admin_audit` gets `prev_hash` and `row_hash`
  columns, plus a `BEFORE INSERT` trigger that:
  - reads the previous row's `row_hash` (or genesis zero-bytes),
  - builds a canonical string from `id|actor|actor_id|action|target_id|details|at`,
  - computes `row_hash = sha256(prev_hash || canonical)` via `pgcrypto.digest`.
- **Migration 0011** — wraps the trigger body in
  `pg_advisory_xact_lock('admin_audit'::regclass::oid::bigint)`.
  Transaction-scoped advisory lock keyed on the table's OID; concurrent
  INSERTs serialize through the SELECT-then-INSERT critical section.
  Released at COMMIT or ROLLBACK.
- **Migration 0012** — extracts the canonical-string computation into a
  shared SQL function `admin_audit_canonical(id, actor, ...)` so trigger
  and verifier compute byte-identical bytes. Then rewrites
  `VerifyAuditChain` to check both linkage **and** content: for every
  row, recompute `digest(prev_hash || canonical(row), 'sha256')` and
  compare against the stored `row_hash`. Any mismatch contributes to
  `broken_links`.

The verifier runs as a 5-minute background tick in `internal/audit/verifier.go`,
publishing `audit_chain_broken_links` to Prometheus and logging WARN on
non-zero. An on-demand check is exposed at `GET /v1/admin/audit/verify`.

## Alternatives considered

- **SERIALIZABLE isolation** for the admin INSERT path. Rejected: forces
  every admin-touching TX to opt in, leaks isolation concerns up to the
  handler layer, and risks 40001 retries on unrelated writes in the
  same TX.
- **`SELECT ... FOR UPDATE` on a sentinel row.** Rejected: needs a row
  to lock — the table is empty at genesis, and even with a permanent
  sentinel, every admin action would block on it serially, which is
  what `advisory_xact_lock` does without the locator gymnastics.
- **HMAC keyed off an application secret the DB doesn't hold.** This
  is the right shape for *tamper-proof* (vs. tamper-evident) — an
  attacker with DB write access can recompute SHA-256 hashes; they
  cannot recompute HMAC without the key. We did not ship this because
  the brief had no compliance ask and the operator threat model is
  "accidental corruption + naive UPDATE attempts," not "DBA-level
  attacker." Migration 0011's comment is explicit that this is
  tamper-evident, not tamper-proof.
- **External attestation** (notarize chain heads to S3 / a
  transparency log). Same threat-model gap as HMAC; same answer.
- **Append-only DB role for `admin_audit`.** A separate Postgres role
  with INSERT-only privilege on `admin_audit` would prevent the app
  role from UPDATE/DELETE on the chain. Defensible for production;
  not shipped because role separation isn't done elsewhere in this
  codebase and one half-measure is misleading.

## Consequences

- **Tamper-evident across content + linkage.** The verifier returns
  `broken_links > 0` if any row's `prev_hash` is off, OR any row's
  `row_hash` doesn't match the recomputed digest. An attacker who
  edits `details` in place without rewriting the chain is detected on
  the next tick.
- **Concurrent admin INSERTs serialize through the trigger.** Admin
  volume is low (one INSERT per admin action), so contention is
  negligible. The lock only covers the SELECT-then-INSERT critical
  section, not the surrounding TX, so the broader admin handler isn't
  serialized.
- **Verifier cost is O(n) digests per tick.** Five-minute cadence times
  a low-volume audit table makes this trivially affordable; on a
  million-row table it's ~1 second of CPU. Tighten the interval for
  compliance-heavy deployments.
- **Not tamper-*proof*.** An attacker with both `admin_audit` write
  access and the ability to recompute SHA-256 (which is everyone) can
  rewrite the entire chain. The chain detects accidental and naive
  attacks; it does not detect a determined insider with DB access.
  Real tamper-proof needs HMAC, role separation, or external
  attestation.
- **A trigger without a running verifier is theater.** Migration 0009
  + 0011 alone leave the audit feature tamper-evident in theory but
  with no caller in product code. The 5-minute background tick + the
  admin endpoint are the load-bearing pieces; without them, breaches
  are detectable only "whenever someone reads `VerifyAuditChain`."
