# Design note: centralized work-discovery

Status: **proposal** (post-discovery design, not yet implemented)
Scope: `scoop-core` event loop, with `scoop-quarkus` wiring
Relates to: the v0.4.0 NOTIFY-gated reconciliation work (this is its natural completion)

> This note was written after reading the current code. Where the originating brief and
> the code disagreed, the code won; the findings below are what the code actually does as
> of this branch. File:line references are anchors, not guarantees they won't drift.

---

## 1. Problem

Scoop runs the event loop **per coroutine** (per topic/saga type). `EventLoop.tick(topic,
distributedCoroutine, …)` (`scoop-core/.../coroutine/EventLoop.kt:183`) does its work "only for
the passed in `distributedCoroutine`" (docstring, `EventLoop.kt:158`), and each saga is driven
by its own single-thread scheduled executor (`EventLoop.tickPeriodically`, `EventLoop.kt:312`;
executor created at `EventLoop.kt:319`). With **N** registered coroutines you get **N**
independent loops, each firing every `tickInterval` (default 50 ms) and doing two things:

1. **Reconcile** — the `EMITTED→SEEN` / `ROLLBACK_EMITTED→ROLLING_BACK` anti-joins
   (`startContinuationsForCoroutine`, `MessageEventRepository.kt:515`).
2. **Resume** — the readiness gate: find *this coroutine's* runs that are ready to advance, then
   execute the next step (`whileISaySo { … fetchSomePendingCoroutineState(…) }`, `EventLoop.kt:227`).

### What 0.4.0 already fixed, and what it left

v0.4.0 gated **reconcile** on LISTEN/NOTIFY (`ReconcileGate`, `EventLoop.kt:197`): a worker only
runs the anti-joins after a `pg_notify` for its topic (plus a rare safety-net sweep, default 30 s).
Idle topics now do ~zero reconcile scans.

The **resume** half is *not* gated — it runs on **every tick of every coroutine**, unconditionally
(the `whileISaySo` block at `EventLoop.kt:227` is outside the `if (reconcileGate.shouldReconcile())`
guard). This is deliberate: time-based readiness (deadlines, sleep, retry backoff) and
child-completion readiness have **no triggering NOTIFY today** (see §7), so the loop must poll.

Net idle cost is therefore `O(N coroutines × poll cadence × per-resume-query cost)`, almost all of
it returning zero rows. Two amplifiers observed downstream (nexus), to confirm in scoop's own
benchmarks:

- The resume drain runs every tick (above).
- `whileISaySo` (`EventLoop.kt:227`) re-runs the gate back-to-back while it keeps returning a ready
  run (`saySo()` at `EventLoop.kt:245`) — so one active coroutine spins its heavy query at
  query-rate.

### Why the per-coroutine query is so redundant (the key finding)

The resume gate is built in `PendingCoroutineRunSql.kt` as a CTE chain
(`candidateSeens → latestSuspended → … → candidateSeensWaitingToBeProcessed → seenForProcessing →
finalSelect`). Two facts, verified in the code, make N of these almost identical:

1. **The only per-coroutine predicate is one bound parameter.** The entire chain is keyed off
   `candidateSeens`, whose sole coroutine-specific filter is
   `WHERE seen.coroutine_name = :coroutine_name` (`PendingCoroutineRunSql.kt:92`). Everything
   downstream flows from that CTE. Nothing else in the resume path mentions the coroutine.

2. **The strategy fragments carry no per-coroutine literals.** The gate interpolates four SQL
   fragments from the `EventLoopStrategy` (`candidateSeensWaitingToBeProcessed`,
   `PendingCoroutineRunSql.kt:333,337,363,367`):
   - `resumeHappyPath` / `resumeRollbackPath` → `allEmissionsHaveCorrespondingContinuationStarts`
     (`strategyBuilders.kt:162`), which embeds the **entire** handler topology as a `VALUES (…)`
     literal from `getTopicsToHandlerNames()` (`strategyBuilders.kt:183`) — the *whole registry*,
     not this coroutine's slice. Same registry ⟹ byte-identical text.
   - `giveUpOnHappyPath` / `giveUpOnRollbackPath` (`EventLoopStrategy.kt:146,161`) → cancellation +
     deadline fragments (`strategyBuilders.kt:43,107`), which reference only CTE aliases and
     constant key names. No per-coroutine literals.
   - `ignoreOlderThan` *is* interpolated as a literal (`strategyBuilders.kt:25`) — but only in
     `start()`, which feeds the **reconcile** anti-join, **not** the resume gate. The resume gate
     text is independent of `ignoreOlderThan`.

**Consequence:** for all coroutines using `StandardEventLoopStrategy` against one shared
`HandlerRegistry`, the generated resume-gate SQL is **identical except for the bound
`:coroutine_name`**. nexus's ~80 coroutines emit ~**2** distinct gate texts (Standard + the internal
`SleepEventLoopStrategy`), executed ~80× per cadence differing only in one parameter — the textbook
case for collapsing `N` parameterized executions into `~K` shared ones.

---

## 2. Proposed shape: separate discovery from execution

Today each coroutine loop both **discovers** its own ready work (runs the gate) and **executes** it
(claims the row, runs the step). Split them:

- **One central discoverer** runs **one query per gate-fingerprint group** (see §3), finding
  ready/due runs across *all* coroutines in that group in a single pass, returning
  `(coroutine_name, message_id, cooperation_lineage)` tuples. **Read-only, no locking.**
- **Execution stays per-run, unchanged.** Each discovered tuple is handed to the existing
  claim-and-run path — `fetchPendingCoroutineRun` (`MessageEventRepository.kt:746`) →
  `FOR UPDATE SKIP LOCKED` claim (`seenForProcessing`, `PendingCoroutineRunSql.kt:407`) →
  double-checked re-validation (`secondRunAfterLock`, `PendingCoroutineRunSql.kt:412`) →
  `resumeCoroutine`. All structured-cooperation semantics (lineage, suspension, rollback) live
  here and are untouched.

The structured-cooperation invariants live in the **execution** layer. Discovery only answers
"which runs *might* be ready"; the claim's double-checked lock is already the authority on "is it
*still* ready," so a discoverer that occasionally over-reports is safe by construction.

```
                 ┌──────────────────────────────────────────┐
   NOTIFY  ─────▶│  Discoverer (1 thread)                    │
   timer   ─────▶│   • one read-only query per gate group    │
                 │   • emits (coroutine, message_id) items   │
                 └───────────────┬──────────────────────────┘
                                 │  bounded, fair dispatch queue
                                 ▼
                 ┌──────────────────────────────────────────┐
                 │  Execution pool (bounded workers)         │
                 │   • per-item: existing SKIP-LOCKED claim  │
                 │     + double-checked lock + resumeCoroutine│
                 │   • per-coroutine in-flight cap = instances│
                 └──────────────────────────────────────────┘
```

### What this buys (and what it does not)

Three distinct wins, in rough order of impact:

1. **The heavy projection moves from "every idle poll" to "only on a hit."** Today the resume drain
   calls `fetchPendingCoroutineRun` (`MessageEventRepository.kt:751`), whose **first pass computes
   the full `finalSelect`** — `lastTwoEvents` + the correlated `JSON_AGG` subqueries for child
   exceptions / executed-step-instances (`PendingCoroutineRunSql.kt:482–545`) — and then extracts
   only `id`, discarding the rest. So **every idle tick of every coroutine** computes that heavy
   projection and gets nothing. Discovery stops at `candidateSeensWaitingToBeProcessed` (predicate
   only, no `finalSelect`); the expensive projection runs **only** for runs that are actually about
   to execute. This is the largest single saving and is independent of the N→K argument.
2. **Per-query fixed cost × N → × K.** Parsing/planning one large statement N times becomes K times
   (`K = #gate-fingerprints`, ~2–3). The win is **not** less data scanned — the union of all
   coroutines' candidates is the same rows — it is removing the redundant parse/plan and the
   per-coroutine `whileISaySo` busy-spin × N.
3. **Plan stability (bonus).** The central discovery query needn't carry `coroutine_name = $1`, so
   there is one plan per group to tune. Downstream (nexus), the per-coroutine parameterized query
   reportedly suffered a generic-plan/stale-stats flip that reset on each autovacuum/analyze; a
   less-parameterized statement is less exposed. (The "~20 KB / ~450 bound params" figure is a
   downstream observation — note that the handler topology is interpolated as **text literals**, not
   bound params, so most of the 20 KB is `VALUES (…)` size, not param count; confirm the real param
   count in scoop before leaning on this.)

Explicitly **not** solved (don't oversell):
- **Per-scan cost over live history.** One discovery query over a huge `message_event` is still a
  scan — just one per group instead of N. And it is **not guaranteed cheaper per execution than a
  small per-coroutine query**: with the `coroutine_name` anchor gone, the candidate set is the
  global non-terminal-SEEN set, and the plan may change *qualitatively* — possibly **better** (one
  shared hash anti-join for the terminal-state / child-completion `NOT EXISTS` checks instead of N),
  possibly **worse** (a nested-loop blow-up if the planner correlates per candidate). This **must
  be checked with `EXPLAIN ANALYZE` at representative candidate volume**, and it raises the
  importance of indexes supporting `cooperation_lineage` equality (the terminal-state subqueries,
  `PendingCoroutineRunSql.kt:97–120`) and `<@` containment (child detection) — see §9. Bounding
  table growth (pruning/archive/finite window) remains a separate, complementary, app-side lever.
- **Polling itself.** `N→K` still leaves a periodic scan. Killing the scan is §7 — and note §9's
  observation that an event-driven conversion gets there with *fewer* invariant changes.

---

## 3. The central query: one per gate-fingerprint group

**Grouping key.** Because strategy fragments are interpolated as **text**, two coroutines can share
a central query **iff their generated gate SQL is identical**. So the grouping is mechanical, not a
hand-maintained "archetype" taxonomy:

```
fingerprint(coroutine) = hash(generated resume-gate SQL with :coroutine_name removed)
groups = registered coroutines grouped by fingerprint
```

For each group, run **one** query. In scoop core there are exactly two `EventLoopStrategy`
implementations that reach production — `StandardEventLoopStrategy` and `SleepEventLoopStrategy`
(`Sleep.kt:164`) — so the floor is `K = 2`. Downstream apps that subclass `EventLoopStrategy` with
distinct resume/give-up predicates add one group per distinct text; nexus is ~3. Confirm the count
downstream by fingerprinting the registry, not by guessing.

> Why fingerprint instead of `UNION`-ing every strategy into one statement? A `UNION` of
> heterogeneous gate texts would be one monster plan with the worst-case shape of all groups;
> fingerprint-grouping keeps each plan tight and lets Postgres cache `K` small plans. `UNION` only
> helps if `K` is large *and* the shapes are near-identical — measure before reaching for it.

**Transform of the existing SQL** (resume gate, `PendingCoroutineRunSql.kt`):

1. In `candidateSeens` (`:92`), **drop** `WHERE seen.coroutine_name = :coroutine_name`. The CTE now
   ranges over every non-terminal `SEEN` across all coroutines in the group. (For groups that are a
   *subset* of the registry — e.g. mixed strategies in one JVM — replace the equality with
   `seen.coroutine_name = ANY(:coroutine_names)`, one bound array per group, still one plan.)
2. **Project** `seen.coroutine_name` through the chain (it already carries `seen.*` at `:74`; expose
   it in `candidateSeensWaitingToBeProcessed`'s select at `:319` and in the discovery projection).
3. **Drop** the `FOR UPDATE SKIP LOCKED LIMIT 1` tail (`seenForProcessing`, `:407`) from the
   *discovery* query. Discovery returns the *set* of ready `(coroutine_name, message_id,
   cooperation_lineage)`; it does not lock. Locking happens in execution (step 4).
4. **The claim path is *not* a free reuse of `fetchPendingCoroutineRun` — correct it.** Today
   `fetchPendingCoroutineRun` (`MessageEventRepository.kt:751`) runs a first pass that is the **full
   per-coroutine gate with `FOR UPDATE SKIP LOCKED LIMIT 1`** and locks *whatever* run is ready
   first — it does **not** target a particular run. Invoking it per discovered item would (a)
   re-run the whole expensive gate per item (defeating the point) and (b) possibly lock a
   *different* ready run than the one discovered. Instead, the claim must be **anchored to the
   discovered run**:
   - **Pass 1 (lock):** `candidateSeens` filtered by **`seen.cooperation_lineage = :lineage`** (the
     run is uniquely identified by its lineage; `message_id` alone is **not** unique — one message
     can have a SEEN per handler/coroutine), keeping `FOR UPDATE SKIP LOCKED`. This locks exactly
     the one discovered row — indexed, O(1), not a gate re-scan.
   - **Pass 2 (re-validate + project):** the existing `secondRunAfterLock = true` branch
     (`PendingCoroutineRunSql.kt:412`) anchored on the same run, which re-evaluates readiness after
     the lock (the double-check) **and** runs the heavy `finalSelect` to build the execution
     context.
   So `candidateSeens` gains an optional `:lineage` anchor for the claim path. The claim path also
   **keeps `coroutine_name`** (it is claiming a specific coroutine's run); only the *discovery*
   query drops it. This is a small, bounded change — not "unchanged."

Everything between `candidateSeens` and `candidateSeensWaitingToBeProcessed` — the child-emission /
child-seen / terminated-child CTEs and the structured-cooperation `NOT EXISTS` over
`terminated_child_seens` (`:341`) — is **unchanged**. It was never coroutine-specific; it keys on
`cooperation_lineage`.

**Livelock guard.** Discovery and the claim's pass-2 re-validation **must be built from the exact
same `candidateSeensWaitingToBeProcessed` CTE** (same strategy fragments), so they can never
*systematically* disagree. If discovery used a looser/reimplemented predicate, it could keep
surfacing a run the claim always rejects → discover→reject→rediscover livelock. Sharing the predicate
makes any disagreement purely transient (the run's state genuinely changed between the two reads),
which is self-correcting. Concretely: derive the light discovery projection by truncating the *same*
SQL builder chain at `candidateSeensWaitingToBeProcessed`, don't hand-write a parallel predicate.

**Ordering / fairness in the query.** The current `seenForProcessing` orders by emission time
(`:406`) and takes one row. The discoverer should instead return many rows with a fairness-aware
ordering or post-process for fairness (§5) — e.g. `ORDER BY coroutine_name, COALESCE(rollback_emitted_at,
emitted_at)` and round-robin on dispatch, or a windowed `LIMIT` per coroutine to cap head-of-line
greed.

---

## 4. Multi-instance / claiming (must not regress)

Today, per-coroutine loops + `FOR UPDATE SKIP LOCKED` distribute work across instances and nodes for
free: every worker runs the same gate and races on the row lock; the loser skips
(`PostgresMessageQueue.kt` docstring `:119`; test `PostgresMessageQueueTest` "fans work out across
distinct instance UUIDs"). The distribution unit is the **SEEN row**, not the saga.

The split preserves this **without leader election**:

- **Discovery is read-only and idempotent.** Run it on **every node**. Redundant discovery across
  nodes is just a cheap repeated read; it commits nothing.
- **Claiming stays where it is.** Two nodes that both discover the same item both attempt the
  `FOR UPDATE SKIP LOCKED` claim (`PendingCoroutineRunSql.kt:407`); exactly one wins, the other's
  `LIMIT 1` returns empty and it drops the item. Identical race, identical resolution as today.
- **Instance UUIDs survive.** The executing worker stamps its own `DistributedCoroutineIdentifier.
  instance` on the `message_event` rows it writes, exactly as now.

So the multi-instance story is **"all nodes discover, all nodes claim via SKIP LOCKED."** Leader
election and keyspace partitioning are explicitly *not* required (and would be regressions — a
single discoverer node would be a SPOF for discovery). This keeps scoop a library, not a
leader-elected service.

**`instances` changes meaning — concurrency cap, not N pre-created identities (an invariant
change, call it out).** Today `instances = N` spins N single-thread workers for one saga
(`PostgresMessageQueue.kt:156`), **each carrying a distinct pre-allocated
`DistributedCoroutineIdentifier.instance` UUID**. The bound on concurrency is preserved by capping
in-flight executions per `coroutine_name` at its configured `instances` (default 1) in the
dispatcher (§5). But the *identity* story shifts: a generic execution pool has no per-worker
pre-allocated instance UUID, so the property "**exactly N distinct instance UUIDs participate**"
(asserted by `PostgresMessageQueueTest`'s "fans work out across distinct instance UUIDs") does not
hold for free.

This is **correctness-safe**: `coroutine_identifier` (the instance UUID) is verified **write-only**
— it appears only in `INSERT INTO message_event (… coroutine_identifier …)` and is **never** in a
`WHERE`/`JOIN` or readiness predicate (the gate keys on `coroutine_name`; work partitioning is done
entirely by `FOR UPDATE SKIP LOCKED` on the row). So the instance UUID is **attribution only**
(logs, traces, the `coroutine_identifier` column). Two clean options, decide explicitly:
- **(a) Pre-allocate N identities per coroutine** and round-robin executions across them — preserves
  the "N distinct UUIDs" property and the existing test verbatim.
- **(b) Stamp a per-execution (or per-pool-worker) identifier** and **restate** the test's
  assertion to "N concurrent executions" rather than "N distinct pre-seeded UUIDs."

Recommend (a) for the least behavioral surprise. Either way, the *guarantee that matters* — at most
`instances` concurrent runs of a coroutine, and no two workers processing the same SEEN (SKIP
LOCKED) — is preserved.

---

## 5. Fairness, backpressure, blast radius

Per-coroutine loops get fairness for free (one OS thread each) and isolation for free (a wedged saga
stalls only itself). A central discoverer must re-earn both.

- **Discovery ≠ execution thread.** Discovery is one thread running a fast read-only query and
  enqueuing items. It must never run a step body. A slow step can then never block discovery.
- **Bounded execution pool.** A fixed worker pool drains the dispatch queue. No "dump thousands of
  ready runs into unbounded concurrency." Pool size is a config (default e.g. `min(coresish,
  Σ instances)`).
- **Per-coroutine cap + round-robin.** The dispatch queue caps in-flight items per `coroutine_name`
  (= that coroutine's `instances`, §4) and round-robins across coroutines, so one hot coroutine
  cannot starve the rest (the property per-coroutine loops gave implicitly).
- **Blast radius.** One discoverer wedging stalls *every* saga's *new* discovery (in-flight
  executions still finish). It must be the most robust component: conservative predicates (it only
  ever *reads* and proposes; the claim re-validates), generous timeouts, and a watchdog that
  restarts the discovery thread. There is prior art for a "reaper deletes runs it shouldn't" class
  of bug downstream — discovery must stay strictly non-destructive.
- **Infra-failure backoff stays per-run.** The `backoffUntil` / `backoffAttempts` maps live in
  `EventLoop` (`EventLoop.kt:145`) and gate a specific `messageId` (`EventLoop.kt:242`). They move
  with execution, not discovery; discovery may surface a backed-off run, and the executor skips it
  exactly as `isBackedOff` does today.

---

## 6. Semantics preserved (tests that must stay green)

Discovery changes *who asks* and *how often*, not *what counts as ready*. The gate predicate text is
reused verbatim (minus `:coroutine_name`), so the following must remain green unchanged
(`scoop-quarkus/.../structuredcooperation/`):

- `LoopTest` — Repeat/Continue/GoTo, per-iteration suspend/resume, child-batch waiting, rollback in
  reverse iteration order, `childFailureHandlerIteration` tracking.
- `StubHandlerBlockingTest` — a parent must **not** resume until every expected handler has a SEEN
  (this is exactly `allEmissionsHaveCorrespondingContinuationStarts`, preserved).
- `HappyPathTest` / `RollbackPathTest` — child completes before parent resumes; rollback ordering.
- `DeadlineTest`, `SleepTest`, `CancellationTest` — the time/cancel give-up fragments
  (`strategyBuilders.kt`), preserved verbatim.
- `InfrastructureFailureRetryTest` — infra exception retries (not rolls back); backoff per run.
- `GoToTest`, `MessageEventsTest`, `PendingCoroutineRunSqlTest` — gate-SQL correctness and event
  ordering.
- `PostgresMessageQueueTest` — multi-instance fan-out across distinct instance UUIDs;
  `requiredConnectionCount` reflecting registered workers (this number *changes* under the new model
  — see §8 — so that specific assertion will be re-stated, not the fan-out behavior).

New tests to add: (a) one central query returns ready runs for *several* coroutines in one pass;
(b) two simulated nodes both discover one item, exactly one claims it (SKIP-LOCKED race);
(c) per-coroutine `instances` cap bounds concurrency under the central pool; (d) discoverer-thread
death is detected and restarted without losing in-flight executions.

---

## 7. Future direction: poller → timer (design toward, don't build yet)

`N→K` removes redundancy but still **polls**. To kill the scan, separate the two reasons the resume
path polls at all — both verified to lack a triggering NOTIFY today:

1. **Child-completion readiness.** A child committing inserts a `COMMITTED` `message_event` with
   **no `message` row**, so the V1 `message`-insert trigger
   (`V1__create_message_table.sql` `notify_message_insert`) does **not** fire; and the V5 trigger
   fires only on `ROLLBACK_EMITTED` (`V5__notify_on_rollback_emitted.sql`). So a parent learns its
   children finished **only by polling**. *Fix path:* add a trigger that `pg_notify`s the parent's
   topic on terminal child events (`COMMITTED` / `ROLLED_BACK` / `ROLLBACK_FAILED`), symmetric with
   how emissions already notify. Then child-completion becomes event-driven and the discoverer wakes
   on it instead of scanning for it.
2. **Time-based readiness.** Deadlines (`strategyBuilders.kt:107`), sleep-until
   (`Sleep.kt` `SleepEventLoopStrategy`), retry backoff (`EventLoop.kt:145`). These are pure
   functions of the clock — a **min-heap scheduler** serves them exactly: compute each pending run's
   next-due instant, sleep until precisely then, wake and dispatch. Near-zero idle, no scanning.

So the asymptote is: **NOTIFY** (new work + child completion) + **timer** (time readiness) ⟹ the
periodic scan disappears. Design the discoverer so it *owns* "when is the next thing due": in v1 it
answers that with a periodic gate scan; in v2 it answers from an in-memory schedule it maintains
(seeded by a scan, kept fresh by NOTIFY + each dispatch's recomputed next-due). **Do not build the
v1 poller as a dead end** — give it that "next-due" interface from the start.

---

## 8. Open decisions (for the implementer to settle in code)

1. **Fingerprint vs explicit groups.** Auto-fingerprint the generated gate SQL (robust, zero
   config) vs. let strategies declare a group id. Recommendation: fingerprint; it can't drift from
   the actual SQL.
2. **Subset groups & the `coroutine_name = ANY(:names)` variant.** Only needed when one JVM mixes
   strategies whose registries differ; for a single shared registry the gate is global and the
   filter drops entirely. Decide whether to always pass the array (uniform) or specialize the
   "whole registry" case (one fewer bound param, marginally better plan).
3. **Discovery cadence in v1.** Keep a fixed interval (e.g. the current 50 ms) for time-readiness
   until the §7 timer lands; the per-scan cost is now paid `K` times, not `N`, so a *shorter*
   interval may even be affordable. Measure.
4. **Execution pool size & connection budget.** `requiredConnectionCount` changes from
   `Σ instances` (`PostgresMessageQueue.kt:233`) to `1 (discovery) + pool_size`. Decide
   `pool_size` (cap at `Σ instances` to preserve max concurrency, or smaller to bound DB load) and
   restate the `requiredConnectionCount` test/assertion accordingly.
5. **Reconcile must be *re-homed*, not "left as-is" (the note's earlier framing was wrong).**
   Reconcile (`startContinuationsForCoroutine`) is genuinely per-coroutine — `:coroutine_name` at
   `MessageEventRepository.kt:532,585` and `ignoreOlderThan` interpolated as a literal — so keep its
   *logic* per-coroutine (don't fold it into a fingerprint group; the per-coroutine `ignoreOlderThan`
   literals would fragment grouping anyway). **But it currently lives *inside* the periodic
   per-coroutine `tick` (`EventLoop.kt:197`) that this design removes.** So when the periodic tick
   goes away, reconcile's *trigger* must be rehomed to two surviving sources, preserving exactly
   today's semantics:
   - **NOTIFY-driven:** a `pg_notify` for topic T runs reconcile for the coroutine(s) on T (this is
     already how 0.4.0 makes it idle-free; the notifier callback now drives reconcile directly
     instead of via a gated tick). The `ReconcileGate` "drain across `QUIET_TICKS`" behavior
     (`ReconcileGate.kt`) and the 0.4.0 sibling-fanout fix must be carried over or shown unnecessary.
   - **Safety-net sweep:** the 30 s `scoop.reconcile.safety-net-interval` fallback was *also*
     implemented via the periodic tick (`ReconcileGate.shouldReconcile()` returning true when the
     interval elapsed). Removing the tick removes that fallback, so a **standalone periodic
     reconcile sweep** (every safety-net interval, all coroutines) must be added. It is cheap (30 s,
     not 50 ms) but it is **not free and must not be forgotten** — without it, a missed NOTIFY is
     never recovered.
   Net: reconcile stays per-coroutine in logic; its periodic-tick host is replaced by
   {NOTIFY callback + a coarse safety-net sweep}. Resume is what gets centralized.
6. **Where the discoverer lives.** A new `WorkDiscoverer` in `scoop-core` owning the registry view
   (`HandlerRegistry.listenersByTopic`, `PostgresMessageQueue.kt:205`) + the dispatch pool; wired in
   `scoop-quarkus/ScoopProducer`. `PostgresMessageQueue.subscribe` stops creating a per-saga
   `tickPeriodically` and instead registers the saga with the discoverer. The internal
   `sleep-handler` subscription (`PostgresMessageQueue.kt:81`) becomes just another registered
   coroutine in its own fingerprint group.

---

## 9. Critical review — invariant ledger, residual risks, and a lower-risk alternative

This section is an adversarial pass over the design itself (verified against the code), so the
implementer inherits the caveats, not just the happy path.

### 9a. Invariant ledger (what is preserved vs. what genuinely changes)

| Invariant | Status | Why |
|---|---|---|
| `global_result == ⋃_C per_coroutine_result(C)` | **Preserved** | Readiness of a run is a function of its `cooperation_lineage` + global event state + the (shared) registry, **not** of the bound `coroutine_name` — the only per-coroutine predicate is the `candidateSeens` anchor (`PendingCoroutineRunSql.kt:92`); every downstream CTE and strategy fragment keys on lineage or reads globally. Dropping the anchor enlarges the candidate set; it does not change any per-run verdict. |
| Structured-cooperation rule (parent waits for all children to terminate) | **Preserved** | The child-emission/child-seen/terminated-child CTEs already read across coroutines and key on lineage; untouched. |
| No double-processing of a SEEN | **Preserved** | `FOR UPDATE SKIP LOCKED` + double-checked re-validation stay in the claim path (now lineage-anchored). |
| Multi-instance / multi-node scaling | **Preserved, no leader election** | All nodes discover (read-only, idempotent); all race the per-row claim; SKIP LOCKED resolves. Equivalent to today's per-coroutine race. |
| At most `instances` concurrent runs per coroutine | **Preserved** | Re-expressed as a per-`coroutine_name` dispatch cap (§5). |
| "Exactly N **distinct instance UUIDs** participate" | **Changes** (correctness-safe) | `coroutine_identifier` is write-only/attribution (verified: never in a predicate). Pre-allocate N identities (option a) to keep it, or restate the test (option b). §4. |
| Per-coroutine FIFO-ish processing (emission order) | **Must be re-established** | Today `whileISaySo` + `ORDER BY emitted_at` (`PendingCoroutineRunSql.kt:406`) gives per-coroutine emission order. With `instances`-cap = 1 the dispatcher must process a coroutine's ready runs in emission order to preserve it (independent runs, so this is behavioral parity, not a hard correctness rule — but tests may observe it). |
| Reconcile semantics (NOTIFY-driven + 30 s safety-net) | **Preserved only if re-homed** | The periodic tick that hosts both is removed; both triggers must be rebuilt (§8.5). Easy to forget the safety-net sweep → silent loss of missed-NOTIFY recovery. |
| Infra-failure backoff (per-run) | **Preserved, already concurrent** | `backoffAttempts`/`backoffUntil` are `ConcurrentHashMap` on a single shared `EventLoop`, keyed by `messageId` — already touched by multiple coroutine threads today, so a shared pool adds no new hazard. |

### 9b. Residual risks / things that must be checked (not assumed)

1. **The global plan can be worse, not just "slower once."** With `coroutine_name` gone, the
   candidate set is global and the terminal-state `NOT EXISTS` correlated subqueries
   (`PendingCoroutineRunSql.kt:97–120`, equality on `cooperation_lineage`) and `<@` child checks run
   over more rows. Postgres *may* rewrite these into single shared hash anti-joins (a win) or pick a
   per-candidate nested loop (a blow-up). **`EXPLAIN ANALYZE` at representative candidate volume is a
   gating step**, and it elevates the need for a **btree supporting `cooperation_lineage` equality**
   (array `=` does **not** use the existing GIN, which only serves `@>`/`<@`/`&&`). Add the index if
   the plan shows it; this is a pre-existing exposure that centralization concentrates into one
   statement.
2. **Re-discovery duplicate storms (backpressure + dedup).** Discovery re-runs on every NOTIFY and
   every timer/interval; it re-surfaces every ready run **not yet drained**. Without dedup the
   dispatch queue fills with duplicates and the pool wastes claims on already-in-flight runs (SKIP
   LOCKED makes them harmless but not free). Mitigate with **(i)** an in-flight/queued set keyed by
   `cooperation_lineage` that discovery skips, and **(ii)** capacity-aware discovery (fetch ≤ free
   pool slots, e.g. a `LIMIT` tied to available capacity). This is new machinery the per-coroutine
   `whileISaySo` never needed.
3. **Multi-node claim contention.** Every node discovers the full ready set and tries to claim all of
   it → `nodes × items` claim attempts for `items` rows. This equals today's `nodes × per-coroutine`
   contention (no regression), but **randomize per-node dispatch order** so nodes don't all collide on
   item 0 first.
4. **Blast radius is larger than per-coroutine loops.** A wedged discoverer stalls *all* new
   discovery (in-flight executions still finish). It must be the most robust component: strictly
   non-destructive (read + propose only; the claim re-validates), watchdog-restarted, conservative
   predicates. There is downstream prior art of a "reaper that deletes runs it shouldn't" — discovery
   must never delete or write.
5. **Observability regression.** The per-coroutine `scoop-tick-<identifier>` threads
   (`EventLoop.kt:319`) give free per-coroutine CPU attribution in thread dumps; a shared discoverer +
   generic pool loses that. Add per-coroutine counters (discovered / claimed / executed / claim-miss)
   to replace what the thread names gave, especially since the brief cares about Grafana monitoring.
6. **Test-fixture & lifecycle plumbing to re-home:** `pauseTicks`/`resumeTicks` (TRUNCATE-deadlock
   avoidance, `PostgresMessageQueue.kt`) must pause the discoverer **and** the pool;
   `requiredConnectionCount` changes from `Σ instances` to `1 (discovery) + pool_size` and its test
   assertion must be restated; `Subscription.close()` / `PeriodicTick.close()` draining moves to the
   pool.

### 9c. The lower-risk alternative (read before committing to centralization)

The biggest "hidden tradeoff" is one of *framing*: **centralizing the query is not the only way to
kill the idle cost, and it is the way that changes the most invariants.** The actual cost driver is
that **resume polls every tick regardless of whether anything changed**. Two reasons it must poll —
both verified to lack a trigger today:

- **child-completion** has no NOTIFY (a child `COMMITTED` writes no `message` row, so no trigger
  fires — see §7), and
- **time-readiness** (deadline/sleep/backoff) has no event at all.

A purely **event-driven per-coroutine** conversion fixes both **without touching any invariant in
the ledger above**: add a child-terminal-event NOTIFY (§7) so a parent wakes when its children
finish, and give each coroutine loop a small **timer/min-heap** for its next time-trigger. Then each
coroutine ticks **only when woken**, idle cost → 0, and `instances`, instance UUIDs, per-coroutine
isolation, fairness-for-free, and the multi-instance story all stay **exactly as they are**.

Against that baseline, query-centralization buys specifically:
- lower cost when **many coroutines wake at once** (a burst, or the safety-net sweep): one K-query
  beats N-queries; and
- lower **per-wake** parse/plan cost if wakes are frequent.

It does **not** buy idle-cost elimination that the event-driven conversion doesn't already give — and
it costs the invariant changes in §9a + the risks in §9b.

**Recommended sequencing** (reframes the brief's v1/v2):
1. **First, make resume event-driven per-coroutine** (child-terminal NOTIFY + per-coroutine timer).
   This is the high-value, invariant-preserving change and is the real fix for the nexus CPU burn.
2. **Then, *if* burst/sweep wake-storms or per-wake parse cost are still measurable**, add
   query-centralization (this document) as an optimization layer — at which point the §7 timer and
   the central discoverer converge anyway (the discoverer *owns* "next due").

If the implementer disagrees and wants centralization first (e.g. because the child-terminal NOTIFY
is judged too invasive), that's a legitimate call — but it should be made **knowing** it trades the
§9a invariants for an efficiency the event-driven path also delivers, not by default.

## 10. Definition of done

- This design note (done; revise as the code is written).
- A `WorkDiscoverer` that runs `K` (fingerprint-grouped) discovery queries and dispatches to a
  bounded, per-coroutine-capped execution pool reusing the existing claim/run path.
- All structured-cooperation tests green (§6), plus the four new tests.
- A benchmark showing idle CPU scaling with `K` (≈ #strategy shapes), not `N` (#coroutines):
  register many idle coroutines under one strategy and show discovery cost is flat in `N`.
- The discoverer exposes a "next-due" seam so the §7 timer is a drop-in v2, not a rewrite.
