# ADR-0009: argon2id over bcrypt for API keys

## Status

Accepted (2026-05-16)

## Context

API keys are bearer credentials. The raw key is shown to the caller exactly once at creation; the database stores only a hash in `api_keys.hashed_key`. Every authenticated request runs through the auth middleware, which must verify the presented key against the stored hash on the request path.

Two constraints pull in opposite directions:

- The hash must be slow enough that a stolen `api_keys` dump is not trivially crackable on commodity GPUs.
- The hash runs on every request, so its cost is a floor under request latency.

A naive design — slow-hash every row in the table — is unworkable because verification cost scales with table size. We address that with a non-secret `key_prefix` column: the first 8 chars of the raw key are stored alongside the hash and used as a B-tree index lookup. The middleware narrows to one or two candidate rows by prefix, then runs the slow hash only against those.

That leaves the choice of slow-hash function. The relevant prior art is bcrypt (1999), scrypt (2009), and argon2 (2015, winner of the Password Hashing Competition).

## Decision

**argon2id** via `golang.org/x/crypto/argon2.IDKey`, parameters `time=2, memory=64MB, threads=1, saltLen=16, keyLen=32`. The stored column value is encoded as `argon2id$<time>$<memory>$<threads>$<base64-salt>$<base64-hash>` so a future parameter bump is self-describing — verification reads the cost factors from the stored string, not from a constant.

Verification decodes the stored record, recomputes the digest with the stored salt and parameters, and compares with `crypto/subtle.ConstantTimeCompare`. Implementation: `internal/api/auth.go` (`HashAPIKey`, `VerifyAPIKey`).

Parameter rationale: `m=64MB, t=2, p=1` is tuned to ~50ms on a modern x86 core, matching the OWASP Password Storage Cheat Sheet's argon2id baseline. Fast enough that auth is not the dominant cost on a 100ms request budget; slow enough that an attacker with a stolen dump pays ~50ms × candidates × guesses, with memory-hardness making GPU/ASIC parallelism expensive rather than free.

The constant-time compare is not theatrical. A naive `bytes.Equal` on the decoded digest leaks the longest matching prefix via timing; that is the same side-channel class as the Lucky 13 and HMAC-verification CVEs, and it is cheap to defend against here.

## Consequences

- Auth adds a ~50ms floor to every authenticated request. We accept it for the brute-force-resistance guarantee. The next-layer mitigation, if this floor ever bites, is a short-lived bearer cache (Redis) keyed by `<key_prefix>:<short_hash>` with a 60s TTL — that turns repeat-caller cost into one GET. We have not built it yet because we do not need it yet.
- `m=64MB` means each concurrent auth holds 64MB of working memory for ~50ms. At our request concurrency this is well inside container limits; at much higher concurrency the bearer cache above is the answer, not lowering `m`.
- Parameters live in the stored string, not in code. A future cost bump (e.g., `m=128MB`) verifies old records correctly and rehashes lazily on next use.
- The prefix-narrowed lookup pattern means we typically run argon2 against one candidate row, not the whole table. The prefix is non-secret by construction — it is the same first 8 chars an attacker could read from any request — so indexing on it is safe.

## Alternatives Considered

- **bcrypt** (`golang.org/x/crypto/bcrypt`). Mature, well-known, and defensible. Rejected because bcrypt is compute-hard but not memory-hard: a modern GPU cracks bcrypt at cost-12 orders of magnitude faster than argon2id at `m=64MB`. For a new system in 2026, argon2id is the OWASP and PHC-endorsed default, and "we picked the older one because it is more familiar" is not a security argument.
- **scrypt**. Also memory-hard, but the parameter surface (`N, r, p`) is less ergonomic than argon2's explicit time/memory/parallelism triple, and argon2id is the current recommendation. No reason to pick scrypt for a new system.
- **SHA-256 or HMAC-SHA-256 of the raw key**. Deterministic and fast — which is exactly the problem. A stolen `api_keys` dump becomes a rainbow-table attack; an attacker precomputes hashes of every plausible key and matches against the column. Unacceptable for a bearer credential.
- **Plain-text storage**. Not considered seriously. A DB dump or a logged query string becomes a total compromise.
- **Pepper (server-side secret mixed into the hash)**. A reasonable defense-in-depth layer, but it shifts the threat model to "attacker has the DB but not the app secret," which is a narrower window than the one argon2id already addresses. We may add it later; it composes with the current scheme without changing the column format.
