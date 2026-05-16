# ADR-0005: Cursor pagination, never offset

## Status

Accepted (2026-05-16)

## Context

`GET /v1/notifications` returns a paginated list. The `notifications` table is append-heavy and grows without bound — at modest production traffic it reaches millions of rows, and the API must serve "next page" requests with predictable latency regardless of how deep into the history a client is reading.

Two properties matter:

1. **Stable latency.** Page 1 and page 1000 should cost roughly the same.
2. **Stability under concurrent writes.** New rows land constantly. A client paging through results must not see the same row twice or skip rows because the head of the list shifted while they were paging.

Offset pagination satisfies neither.

## Decision

Keyset (cursor) pagination on `(created_at DESC, id DESC)`. We do not expose `?page=N` or `?offset=N` anywhere on the public API.

**Cursor format.** Base64url-encoded JSON `{ts, id}` of the last item from the previous page. The cursor is opaque to clients — they pass it back unchanged in `?cursor=`.

**Query shape.**

```sql
SELECT ...
FROM notifications
WHERE (created_at, id) < ($cursor_ts, $cursor_id)
  AND status = $1            -- dynamic filters compose
  AND channel = $2
ORDER BY created_at DESC, id DESC
LIMIT $page_size + 1;         -- lookahead: N+1 rows ⇒ has_more=true
```

The composite predicate `(created_at, id) < (...)` is the key insight: `created_at` alone is not unique, so a single-column keyset would skip or duplicate rows whenever two notifications share a timestamp. The tuple comparison breaks ties on `id` deterministically.

**Response.**

```json
{ "items": [...], "next_cursor": "eyJ0cyI6Ii4uLiIsImlkIjoiLi4uIn0", "has_more": true }
```

When `has_more` is false, `next_cursor` is omitted.

**Index.** A composite index on `(created_at DESC, id DESC)` backs the cursor predicate and the ORDER BY. The migration in `internal/db/migrations/` adds it; the list handler lives in `internal/api/notifications_list.go`.

## Consequences

- Page latency is O(page_size), not O(offset + page_size). Page 1 and page 10,000 are indistinguishable in cost.
- Pagination is stable under writes: new rows land at the head with larger `created_at`, but the cursor predicate `< (cursor.ts, cursor.id)` only ever moves backward in time, so a paging client never sees a row twice and never skips one.
- Cursors compose with the dynamic filter set (`status`, `channel`, `created_after`, etc.) — the cursor predicate is just one more `WHERE` clause; the planner uses the same composite index.
- The cursor is opaque, so clients cannot construct URLs like `/v1/notifications?page=42`. This is deliberate: any such URL would be stale the moment a new notification lands, and we do not want to support a contract that is broken by design.
- **No "jump to page N."** Cursor pagination is forward/back-from-here only. This is the right tradeoff for a public API; if an internal admin UI eventually needs random-access paging it can use a separate `/admin/notifications` endpoint with an explicit slow-path warning and offset semantics.

## Alternatives Considered

- **Offset pagination (`LIMIT 50 OFFSET 5000`).** Postgres must read and discard 5000 rows to serve page 100 — O(n) per query. On a million-row table, deep pages are unusable. Worse, the result set is non-deterministic under writes: a single new row at the head shifts every page boundary, causing clients to see duplicates and gaps. Rejected.
- **Time-window pagination (`?from=...&to=...`).** Forces clients to know date ranges up front and provides no answer to the basic "give me the next 50" question. UX-hostile and leaks storage layout into the API contract. Rejected.
- **Relay-style GraphQL connections (`edges`, `pageInfo`, `endCursor`).** The specification carries machinery (cursors per edge, bidirectional `before`/`after`, `startCursor`/`endCursor`) that we do not need for a REST endpoint with forward-only paging. Our `{ts, id}` cursor is functionally equivalent to a Relay `endCursor` without the surrounding envelope. Rejected as overkill.
