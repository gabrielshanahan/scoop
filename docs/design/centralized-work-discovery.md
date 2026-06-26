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

The win is **not** less data scanned — the union of all coroutines' candidates is the same rows.
The win is removing **per-query fixed cost × N** (planning one ~20 KB / ~450-param statement N times
becomes planning `~K` statements) and the **per-coroutine busy-spin × N**. Cost moves from
`O(N × cadence × per-query)` toward `O(K × cadence × per-query)`, `K = #gate-fingerprints` (~2–3),
independent of coroutine count.

Bonus — **plan stability.** The central query needn't carry `coroutine_name = $1`, so there is one
plan per group to index and tune. Downstream, the per-coroutine parameterized query also suffered a
generic-plan/stale-stats flip (a per-call latency swing that reset on each autovacuum/analyze); a
single, less-parameterized statement is far less exposed.

Explicitly **not** solved (don't oversell):
- **Scan growth over live history.** One query over a huge `message_event` is still slow — just
  slow once per group instead of N times. Bounding table growth (pruning/archive/finite scan
  window) is a separate, complementary, largely app-side lever.
- **Polling itself.** `N→K` still leaves a periodic scan. Killing the scan is §7.

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
4. **Reuse `seenForProcessing(secondRunAfterLock = true)` unchanged for the claim.** That branch
   (`:412`) already selects a single run by `:message_id` with no `FOR UPDATE` and is exactly what
   the double-checked-lock second pass needs. The first-pass lock (`fetchPendingCoroutineRun`,
   `MessageEventRepository.kt:746`) stays as-is, now invoked per discovered item rather than per
   poll.

Everything between `candidateSeens` and `candidateSeensWaitingToBeProcessed` — the child-emission /
child-seen / terminated-child CTEs and the structured-cooperation `NOT EXISTS` over
`terminated_child_seens` (`:341`) — is **unchanged**. It was never coroutine-specific; it keys on
`cooperation_lineage`.

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

**`instances` keeps its meaning — as a per-coroutine concurrency cap.** Today `instances = N` spins
N single-thread workers for one saga (`PostgresMessageQueue.kt:156`), bounding that saga to N
concurrent runs. In the central model there is one execution pool; preserve the knob by **capping
in-flight executions per `coroutine_name` at its configured `instances`** (default 1) in the
dispatcher (§5). Same guarantee ("one `DistributedCoroutineIdentifier` = one serial worker",
`PostgresMessageQueueTest` / v0.2.9 notes), expressed as a dispatch cap instead of a thread count.

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
5. **Reconcile: centralize too, or leave it?** Reconcile is already NOTIFY-gated and idle-free, so
   it is *not* the cost driver. But its anti-join *is* per-coroutine (`:coroutine_name` at
   `MessageEventRepository.kt:532,585`) and embeds `ignoreOlderThan` as a literal, so it has more
   genuine per-coroutine variance than the resume gate. Recommendation: leave reconcile as-is in
   v1 (event-driven already); revisit only if profiling says otherwise.
6. **Where the discoverer lives.** A new `WorkDiscoverer` in `scoop-core` owning the registry view
   (`HandlerRegistry.listenersByTopic`, `PostgresMessageQueue.kt:205`) + the dispatch pool; wired in
   `scoop-quarkus/ScoopProducer`. `PostgresMessageQueue.subscribe` stops creating a per-saga
   `tickPeriodically` and instead registers the saga with the discoverer. The internal
   `sleep-handler` subscription (`PostgresMessageQueue.kt:81`) becomes just another registered
   coroutine in its own fingerprint group.

---

## 9. Definition of done

- This design note (done; revise as the code is written).
- A `WorkDiscoverer` that runs `K` (fingerprint-grouped) discovery queries and dispatches to a
  bounded, per-coroutine-capped execution pool reusing the existing claim/run path.
- All structured-cooperation tests green (§6), plus the four new tests.
- A benchmark showing idle CPU scaling with `K` (≈ #strategy shapes), not `N` (#coroutines):
  register many idle coroutines under one strategy and show discovery cost is flat in `N`.
- The discoverer exposes a "next-due" seam so the §7 timer is a drop-in v2, not a rewrite.
