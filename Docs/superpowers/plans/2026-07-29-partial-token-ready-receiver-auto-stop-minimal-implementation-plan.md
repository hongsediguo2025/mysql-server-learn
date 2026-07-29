# Preserve/Resume Partial Token READY and Receiver Auto-Stop Minimal Implementation Plan

> **执行约束：** 生产代码、测试、提交和推送均由主 session 完成；sub-agent
> 只做独立只读审查。按任务逐项执行并更新 checkbox，不并行修改同一工作树。

**Goal:** Implement the converged "方案 2": preserve safely excludable source
tokens before final metadata, return successful `STANDBY_TRANSFER_SAVE` DRAIN
without shutting down the fenced source, publish a receiver-local
READY/EXCLUDED partition at the prewarm cutoff, adopt only READY survivors,
hand excluded recovered transactions to native rollback, and stop idle receiver
workers without destroying promotion-owned resources.

**Architecture:** Keep the current transfer protocol, accepted epoch registry,
strict prepared-token registry, physical bootstrap attempt, SQL RESUME path,
and receiver worker pool. Split only the source success terminal so strict
transfer FINAL_ACK enters a fenced transfer-complete state and returns the
DRAIN result without `shutdown()`; local carrier keeps the existing shutdown
path. Move bounded identity authentication to the existing all-objects-sealed
admission boundary before the staged heavy job is enqueued, without adding a
second pool, queue, registry, or lifecycle. Late/finalized source failures,
protocol ambiguity, identity corruption, and global invariant failures remain
whole-epoch fail-closed.

**Tech Stack:** MySQL 8.0.22 C++, InnoDB recovery hooks, Preserve transfer protocol v1, GUnit, MTR, Python E2E/full-pressure runners.

---

## 1. Fixed Scope

### 1.1 Required behavior

1. A source token may become `SOURCE_EXCLUDED` only before any final metadata
   sequence is assigned and only after source ownership is known to be safe.
2. At receiver classification cutoff:
   - every accepted fact token has completed the identity-first pass;
   - strict-ready tokens become `READY`;
   - non-ready tokens with authenticated resurrection identity become
     `RECEIVER_EXCLUDED`;
   - missing or inconsistent transaction identity fails the entire epoch.
3. The accepted fact remains immutable and continues to describe every token
   accepted from the source:

   ```text
   ready_tokens + receiver_excluded_tokens == accepted_fact.tokens
   ```

4. Physical bootstrap only registers resurrection candidates for receiver
   READY tokens. Receiver-excluded tokens retain authenticated identity facts
   for one batch lookup after `trx_sys_init_at_db_start()`; they deliberately
   scan Undo body through the native non-candidate path.
5. After `trx_sys_init_at_db_start()`:
   - receiver-excluded tokens are converted from recovered Preserve PREPARED to
     recovered ACTIVE before the caller's one-time
     `trx_sys_need_rollback()` evaluation and are left to native recovery
     rollback;
   - READY tokens remain PREPARED and proceed through the existing strict adopt
     gate after DD/MDL is ready;
   - zero READY survivors skip strict adopt without failing ordinary promotion.
6. Receiver workers stop after queues, in-flight/admission work, deferred work,
   and every worker-owned cleanup reference are empty. Retained online/terminal
   epochs do not by themselves keep workers alive. READY resources and TTL
   remain valid.
7. Existing SQL `RESUME PRESERVED TRANSACTION` is unchanged.
8. After strict transfer FINAL_ACK:
   - ownership enters `TRANSFER_HANDOFF`;
   - manager enters `TRANSFER_HANDOFF_COMPLETE` with no DRAIN-THD owner;
   - the structured DRAIN result is returned without calling `shutdown()`,
     `kill_mysql()`, or deferred shutdown signaling;
   - ordinary source commands remain default-denied and transferred
     transactions are never restored, rolled back, or destroyed;
   - only external HA decides later source demotion, explicit shutdown, or
     rebuild. `LOCAL_CARRIER` keeps its current automatic shutdown behavior.

### 1.2 Explicit non-goals

- No wire frame, protocol version, codec, artifact, SQL, error-code, or sysvar
  change.
- No finalized-token late ABORT and no sequence renumbering.
- No generic "any token semantic failure is excludable" policy.
- No separate selection object or duplicate authenticated-identity registry.
- No new `RETIRING` accepted-epoch lifecycle.
- No new `accepted_instance_id` in the first release.
- No second receiver worker state machine.
- No receiver crash replay or durable receiver selection.
- No RESET DRAIN changes.
- No external physical-standby HA call-site implementation in this repository.
- No source post-handoff TTL, background rollback, online rejoin, or automatic
  role transition.
- No `_for_unit_test` production interface.

### 1.3 Hard invariants

```text
SOURCE_EXCLUDED restore redo commit LSN <= COMMIT_EPOCH.source_fence_lsn

accepted_fact.tokens
  == ready_tokens UNION receiver_excluded_tokens

ready_tokens INTERSECT receiver_excluded_tokens
  == empty

final_survivors SUBSET_OF ready_tokens

selection_published == true
  => selection never expands

selection_published == true
  => every accepted_fact token has one authenticated identity

only ready_tokens register resurrection candidates

receiver worker stopped
  != receiver promotion resources destroyed

strict QUERY_EPOCH_STATUS
  == current-process commit/ACK status, not internal selection readiness

artifact_mode == STANDBY_TRANSFER_SAVE AND FINAL_ACK accepted
  => source manager == TRANSFER_HANDOFF_COMPLETE
  => shutdown() and kill_mysql() are not called
  => transferred source transactions remain non-restorable

artifact_mode == LOCAL_CARRIER
  => existing SHUTDOWN_REQUESTED and shutdown behavior is unchanged
```

## 2. File Map and Code Budget

| Responsibility | Files | Production LOC |
|---|---|---:|
| Source pre-final allowlist and survivor fact | `sql/preserve_trx.h`, `sql/preserve_trx.cc`, `sql/preserve_trx_transfer.cc` | 80-130 |
| Transfer terminal/no-shutdown split using existing state machines and command gate | `sql/preserve_trx.h`, `sql/preserve_trx.cc` | 25-45 |
| Sealed-token identity admission, accepted classification, reaper order, cleanup, join-only stop | `sql/preserve_trx_transfer.h`, `sql/preserve_trx_transfer.cc`, `sql/preserve_trx.cc` | 150-230 |
| Promotion READY subset and selected deadline | `sql/preserve_trx_promotion.*`, `sql/preserve_trx_promotion_prepared.*` | 75-115 |
| One-scan native rollback handoff | `storage/innobase/include/trx0preserve.h`, `storage/innobase/trx/trx0preserve.cc` | 70-110 |
| Minimal logs; status files only if existing plumbing cannot express the result | transfer/promotion files; optional `sql/preserve_trx_resource.*`, `sql/mysqld.cc` | 10-20 |
| **Expected implementation target** | | **430-600** |

External HA integration code and tests are not included in this estimate.
Identity-first must be implemented by extracting the existing matcher logic and
reusing the sealed-token admission path within the same 150-230 line receiver
budget. Transfer no-shutdown must split the existing success tail and reuse the
current default-deny command gate within 25-45 lines; it must not add a public
HA API or source cleanup state machine. Lines 600-620 are contingency only for
verified race/cleanup guards; exceeding 620 triggers the existing stop
condition.

## 3. Delivery Units

Each delivery unit must be independently buildable and reviewable. Do not start
the next unit while the current unit has unresolved failures.

### Mandatory Slice Review Gate

This gate applies after the targeted verification and before the commit in
Tasks 2, 2A, 3, 4, and 5, and to Task 6 whenever Task 6 changes production
code. It does not apply to every function-level edit or to a
documentation/lint-only Task 6.

The main session first freezes and records:

```text
slice base commit
exact changed-file list
production diff/stat and net LOC
intended invariant and explicit non-goals
targeted build/GUnit/MTR evidence
same-profile before/after performance evidence for affected metrics
git status --short -uall
```

No implementation change may occur while the review batch is active.
Sub-agents are independent read-only reviewers: they may inspect source, diff,
logs, and metrics, but may not edit files, run record-mode tests, stage,
commit, or push.

Dispatch two reviewers with separate evidence streams:

1. **Correctness and failure-path reviewer**
   - ownership and state transitions;
   - publication linearization, lock order, and idempotency;
   - slice-relevant late worker, TTL, cleanup debt, shutdown, zero READY,
     ambiguous ABORT, allocation failure, and promotion races;
   - no leaked or incorrectly owned PREPARED transaction, pin, staging, proof,
     or accepted epoch after every terminal result.
2. **Convergence, isolation, and performance reviewer**
   - reuse of existing registry, reaper, strict gate, rollback primitive, and
     SQL RESUME;
   - redundant/dead code, temporary diagnostics, unnecessary helpers, and
     test-only production interfaces;
   - no new wire/sysvar/error/state machine/global registry outside Task 0;
   - `preserve_trx_enable=OFF`, local startup, and native hot-path isolation;
   - no new hot-path lock, file/network I/O, wait, long scan, or heavyweight
     atomic operation;
   - no unexplained regression in relevant Phase2, ACK-to-READY/prewarm,
     SQL RESUME, QPS/P99, or resource-zero metrics.

Task 4 also requires a third InnoDB recovery reviewer for Undo,
`rw_trx_list`, PREPARED-to-ACTIVE handoff, recovery rollback, purge, and
write-enable ordering.

The main session verifies every finding against current source and records:

```text
finding | severity | source evidence | disposition | verification
```

The slice is blocked when any of the following remains:

```text
unresolved High/Medium correctness or ownership finding
unclear cleanup owner or failure recovery
unapproved file-scope or production-LOC expansion
OFF-path/native-startup/hot-path behavior change
unexplained relevant performance regression
threshold/timeout/resource/assertion relaxation used to obtain PASS
test-only production interface, duplicate state/registry, or removable dead code
```

After review-driven edits, rerun the affected tests. Re-dispatch at least the
affected reviewer when the delta changes an invariant, file boundary, or
performance path. Reviewer agreement is advisory; only source verification and
real test evidence can pass the gate.

---

### Task 0: Freeze the Scheme-2 Design Boundary

**Files:**
- Modify: `design/preserve-resume-8.0.22-partial-token-ready-and-receiver-auto-stop-design.md`
- Modify: `Docs/superpowers/plans/2026-07-29-partial-token-ready-receiver-auto-stop-minimal-implementation-plan.md`

- [ ] **Step 1: Confirm the implementation boundary against current source**

Record and verify:

```text
transfer protocol version remains 1
accepted epoch remains the only selection owner
baseline: STANDBY_TRANSFER_SAVE FINAL_ACK still enters finish_with_shutdown()
Task 2A postcondition: FINAL_ACK does not shut down source mysqld
Task 2A postcondition: current-process source remains fenced and transferred
  transactions are not restored
LOCAL_CARRIER retains automatic shutdown
only READY tokens become Resurrection candidates
excluded tokens use authenticated facts for one post-trx_sys batch lookup
existing SQL RESUME/current connection THD remains unchanged
worker stop is join-only and does not use m_online_epochs.size()
identity-first reuses sealed-token admission and existing ready-state ownership
strict QUERY_EPOCH_STATUS remains COMMITTED_NOT_READY for an accepted epoch
```

The current-process source fence is not durable across mysqld restart. Before
Task 2A can be enabled, the external HA integration must already revoke the old
source's routing/write role and prohibit in-place restart as primary after a
successful DRAIN. Durable source handoff markers remain outside this slice.

Freeze the empty-result behavior:

```text
initial zero-target transfer:
  abort the empty receiver epoch, publish TRANSFER_HANDOFF_COMPLETE, return an
  empty successful result, do not shutdown

all candidates safely excluded before COMMIT:
  do not send an empty COMMIT_EPOCH; after every restore/ABORT is proven,
  publish TRANSFER_HANDOFF_COMPLETE and return exclusions, do not shutdown

any ambiguous restore/ABORT:
  existing DRAIN failure/fail-closed path, no successful transfer terminal
```

- [ ] **Step 2: Reject scope expansion before code changes**

Do not begin Task 1 if implementation requires:

```text
an independent selection registry
a second receiver worker state enum
a new accepted lifecycle enum
a generic source token-local error taxonomy
a new SQL/wire/sysvar/RESET path
a second identity registry, queue, or dedicated identity worker pool
source post-handoff TTL, rollback worker, transaction destructor, or online rejoin API
```

- [ ] **Step 3: Freeze the identity-first scheduling contract**

Record the following as a Task 3 hard prerequisite:

```text
identity-only work runs at all_objects_sealed admission, not in the worker queue
staged-token heavy prewarm requires an authenticated generation-local identity
existing per-object prewarm remains provisional and keeps its current scheduling
identity missing/corrupt at cutoff fails the epoch
identity complete but heavy incomplete may become RECEIVER_EXCLUDED
```

The generation-local identity must live in the existing receiver ready state
and later move into the accepted epoch publication. Do not add a second
identity registry, queue, worker pool, or accepted lifecycle.

- [ ] **Step 4: Freeze the external-contract impact in these two documents**

Record the following authoritative implementation boundary in the design and
this plan:

```text
internal accepted READY == immutable READY/EXCLUDED selection
zero READY survivor is a legal internal READY selection
strict QUERY_EPOCH_STATUS remains ACK-only COMMITTED_NOT_READY
READY_DEADLINE_ACTIVE owns the immutable selection, not an all-token-ready claim
strict all-or-nothing applies only to the final READY survivor subset
strict transfer FINAL_ACK enters TRANSFER_HANDOFF_COMPLETE and returns DRAIN result
TRANSFER_HANDOFF_COMPLETE blocks ordinary source commands without shutdown
HA control remains responsible for later explicit source shutdown/rebuild
```

Do not add a wire field or global HA decision STATUS. If a future HA product
needs a pre-promotion readiness ratio, it requires a separately reviewed
epoch-scoped API.

The TTL, observability, receiver-UML, and physical-promotion integration
documents contain all-token READY wording from the current source baseline.
They are affected follow-up documents, but they are not edited by this
two-document revision. Before release documentation is declared final, audit
those documents against the implemented behavior; do not use their current
all-ready wording to override this plan while implementing Task 3.

This design and this plan are authoritative for the implementation slice.
Pending synchronization of derived documents does not block Task 3 coding, but
it blocks release-document finalization and any external HA contract sign-off.

- [ ] **Step 5: Read-only review**

Have independent sub-agents review source exclusion, receiver publication/reaper
ordering, sealed-admission identity independence, internal-READY/wire-status separation,
promotion/native handoff, and worker lifetime. The main session adjudicates
conflicts; sub-agents do not edit files.

---

### Task 1: Freeze Existing All-or-Nothing and Identity Contracts

**Files:**
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Modify: `mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_lint.test`
- Modify: `mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_lint.result`

- [ ] **Step 1: Add a GUnit contract that finalized tokens remain non-excludable**

Retain and extend the current finalized-token assertion. The test must perform:

```cpp
ASSERT_EQ(session.finalize_token_manifest(token),
          Preserve_trx_transfer_status::OK);
EXPECT_EQ(session.abort_token(token, "late_failure"),
          Preserve_trx_transfer_status::UNSUPPORTED);
```

The expected result is intentionally `UNSUPPORTED`; this plan does not add
late-token sequence surgery.

- [ ] **Step 2: Add a GUnit contract that receiver READY currently requires all tokens**

Create an accepted fact with two tokens, prepare only one strict token, run the
existing READY publication helper, and assert:

```cpp
EXPECT_FALSE(published);
EXPECT_EQ(registry.query_accepted_epoch(root, epoch, &accepted),
          Preserve_trx_transfer_status::COMMITTED_NOT_READY);
```

This test must fail only after the later partial-selection test changes the
expected behavior; keep it in the same change that implements Task 3. Add that
sentence as a test comment so the temporary baseline is not mistaken for the
final product contract.

- [ ] **Step 3: Add source-shape guards for unchanged public surfaces**

The lint test must assert:

```text
kPreserveTrxTransferProtocolVersion remains unchanged
no new SQL grammar token for partial READY
no new preserve_trx sysvar for selection
no new partial-ready/selection/exclusion *_for_unit_test declaration in
production headers; existing unrelated declarations are not broadened
```

- [ ] **Step 4: Run the baseline tests**

Run:

```bash
cmake --build build-debug --target preserve_trx-t mysqld -j8
./build-debug/runtime_output_directory/preserve_trx-t
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test=code_review_resumable_trx_slices_lint --parallel=1 --force
```

Expected:

```text
build succeeds
all existing Preserve GUnit tests pass
code_review_resumable_trx_slices_lint passes
```

- [ ] **Step 5: Commit the contract baseline**

```bash
git add unittest/gunit/preserve_trx-t.cc \
  mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_lint.test \
  mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_lint.result
git commit -m "test: freeze partial-ready safety boundaries"
```

---

### Task 2: Implement Narrow Source Token Exclusion

**Files:**
- Modify: `sql/preserve_trx.h`
- Modify: `sql/preserve_trx.cc`
- Modify: `sql/preserve_trx_transfer.cc`
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Create: `mysql-test/suite/preserve_trx/t/batch_drain_phase2_token_local_exclusion.test`
- Create: `mysql-test/suite/preserve_trx/r/batch_drain_phase2_token_local_exclusion.result`
- Create: `mysql-test/suite/preserve_trx/t/batch_drain_phase2_late_failure_aborts_epoch.test`
- Create: `mysql-test/suite/preserve_trx/r/batch_drain_phase2_late_failure_aborts_epoch.result`

- [ ] **Step 1: Add one explicit pre-final exclusion eligibility**

Do not add a second terminal-owner state machine. Existing
`durable_point_crossed`, `detached_from_original_thd`,
`reattached_to_original_thd`, `cleanup_*`, `left_preserved_*`, stage, and
source rollback image already describe ownership. Add only:

```cpp
enum class Preserve_trx_source_exclusion_eligibility : uint8_t {
  NONE,
  PRE_FINAL_ISOLATED
};
```

and one field in `Preserve_trx_preserve_result`. Set `PRE_FINAL_ISOLATED` only
at the production origin `standby_transfer_resurrection_facts_unsupported`,
and only after the existing prepared-failure cleanup has restored the original
THD-owned ACTIVE transaction without cleanup debt. A Debug injection may
deterministically force this real origin, but no debug-only origin becomes
production-eligible. Never infer eligibility from a failure string, stage
ordering, or cleanup outcome.

- [ ] **Step 2: Add typed exclusion output**

Replace the timeout-only internal vector passed to the DRAIN result writer with:

```cpp
enum class Preserve_trx_exclusion_reason : uint8_t {
  CLOSING_COMMAND_TIMEOUT,
  SOURCE_PRE_FINAL_TARGET_FAILURE
};

struct Preserve_trx_excluded_token {
  uint64_t token{0};
  Preserve_trx_exclusion_reason reason{
      Preserve_trx_exclusion_reason::SOURCE_PRE_FINAL_TARGET_FAILURE};
};
```

Keep the SQL result columns unchanged. Convert the enum to the existing textual
reason at the result-writing boundary.

`preserve_trx_drain_command_timeout_fail_batch` continues to govern only
CLOSING command timeout. Do not reuse it to enable/disable non-timeout
`SOURCE_PRE_FINAL_TARGET_FAILURE`; that path is controlled solely by the
explicit eligibility and ownership/ABORT proof below.

- [ ] **Step 3: Define the only source-local exclusion predicate**

Implement a file-local helper in `sql/preserve_trx.cc`:

```cpp
static bool preserve_trx_source_token_can_be_excluded(
    const Preserve_trx_preserve_result &result) {
  return result.source_exclusion_eligibility ==
         Preserve_trx_source_exclusion_eligibility::PRE_FINAL_ISOLATED;
}
```

The source epoch session already rejects undeclared/finalized/queued-final/
committing tokens. Do not add duplicate coordinator accessors for
`final_metadata_queued` or `ack_uncertain`. After the eligibility check, derive
the actual ownership action from the existing result fields. Reuse the existing
batch/singleton reattach/reactivate helper when the token is detached. Before
sending ABORT, prove that the original THD again owns an ACTIVE transaction and
that cleanup has no ambiguous state; `abort_token()==OK` is the only receiver
proof. Otherwise abort the whole epoch.

- [ ] **Step 4: Reuse existing pre-final `abort_token()`**

Do not add a new transfer frame or late-abort API. Call existing
`abort_token()` only while the token has no queued final metadata.

Treat its result as the existing synchronous frame/ACK outcome; do not add a
new ABORT state:

```cpp
const auto abort_status =
    transfer_session->abort_token(token, "source_pre_final_token_failure");
if (abort_status != Preserve_trx_transfer_status::OK) {
  abort_batch_transfer_epoch(...);
}
```

Ensure `abort_token_locked()` latches `m_ack_uncertain` when the sink returns
`ACK_UNCERTAIN`; later `commit_epoch()` must reject the epoch.

Keep the existing asymmetry explicit:

```text
source_phase1_target_removed:
  UNSUPPORTED may mean the repeated Phase1 scan already aborted this token

closing timeout and SOURCE_PRE_FINAL_TARGET_FAILURE:
  only OK proves this exclusion's receiver ABORT completed
```

Do not copy the Phase1-removal tolerance into the ownership handoff path.
`UNSUPPORTED` also covers undeclared, finalized, already-committing, and
already-aborted states, so it cannot prove a new source exclusion.

- [ ] **Step 5: Freeze exclusions before entering `commit_epoch()`**

Immediately before calling `commit_epoch()` enforce:

```text
every excluded source transaction is ACTIVE on its original THD
every admitted ABORT precedes commit_epoch()
no excluded token owns queued final metadata
at least one finalized survivor remains
```

The executable fence contract is ordering: all source restore and ABORT
operations complete before the call to `commit_epoch()`, whose source-fence
sampling then covers their redo.

If no source survivor remains, abort the entire transfer epoch and return the
existing drain failure/restore outcome. Do not invent an empty COMMIT epoch or
a new all-ABORT apply barrier in this release.

- [ ] **Step 6: Write the positive MTR**

`batch_drain_phase2_token_local_exclusion.test` must:

1. Start two real transactions.
2. Stop one token at an existing deterministic pre-final Phase2 DEBUG_SYNC.
3. Inject a deterministic token-local failure before final metadata.
4. Verify the failed source transaction is ACTIVE and owned by the original
   THD, while ordinary SQL remains subject to the manager fence.
5. Verify DRAIN returns one survivor and one
   `SOURCE_PRE_FINAL_TARGET_FAILURE`.
6. Verify the accepted fact contains only the survivor.
7. Resume and commit the survivor.

Do not use `SLEEP()` for ordering; use existing DEBUG_SYNC/barrier machinery.

- [ ] **Step 7: Write the late-failure fail-closed MTR**

`batch_drain_phase2_late_failure_aborts_epoch.test` must inject failure after
final metadata is queued and assert:

```text
no SUCCESS_WITH_EXCLUSIONS
no COMMIT_EPOCH/FINAL_ACK
all source transactions restored or explicitly left drained on unresolved cleanup
receiver accepted epoch absent
```

- [ ] **Step 8: Run the source slice**

Run:

```bash
cmake --build build-debug --target preserve_trx-t mysqld -j8
./build-debug/runtime_output_directory/preserve_trx-t --gtest_filter='*Abort*:*Exclusion*'
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test='batch_drain_phase2_(token_local_exclusion|late_failure_aborts_epoch)' --parallel=2 --force
```

Expected: all selected tests pass in no-bin mode.

Repeat with:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test='batch_drain_phase2_(token_local_exclusion|late_failure_aborts_epoch)' --mysqld=--log-bin=mysql-bin --parallel=2 --force
```

Expected: both log-bin tests pass with the same ownership result.

- [ ] **Step 9: Pass the mandatory slice review gate**

Freeze the Task 2 diff and evidence, run the two independent read-only reviews
defined above, adjudicate every finding against source, and rerun any affected
test. Do not proceed while a blocking condition remains.

- [ ] **Step 10: Commit the source slice**

```bash
git add sql/preserve_trx.h sql/preserve_trx.cc sql/preserve_trx_transfer.cc \
  unittest/gunit/preserve_trx-t.cc \
  mysql-test/suite/preserve_trx/t/batch_drain_phase2_token_local_exclusion.test \
  mysql-test/suite/preserve_trx/r/batch_drain_phase2_token_local_exclusion.result \
  mysql-test/suite/preserve_trx/t/batch_drain_phase2_late_failure_aborts_epoch.test \
  mysql-test/suite/preserve_trx/r/batch_drain_phase2_late_failure_aborts_epoch.result
git commit -m "preserve_trx: isolate safe pre-final token failures"
```

---

### Task 2A: Return Strict Transfer DRAIN Without Shutting Down Source

**Files:**
- Modify: `sql/preserve_trx.h`
- Modify: `sql/preserve_trx.cc`
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_drain_no_shutdown.test`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_drain_no_shutdown.result`
- Modify: `scripts/resumable_trx_business_e2e.py`
- Modify: `scripts/tests/test_resumable_trx_business_e2e.py`

- [ ] **Step 1: Write the deterministic failing contracts**

Before changing production code, add coverage that demonstrates the current
incorrect behavior:

```text
strict transfer reaches FINAL_ACK and returns a structured DRAIN result
source mysqld remains reachable after that result
ordinary source business SQL receives 4020/default-deny
HA control connection can query status and later issue explicit SHUTDOWN
LOCAL_CARRIER still shuts down
```

The transfer MTR must fail on the current baseline because
`finish_with_shutdown()` calls `shutdown()`. Use real source/receiver mysqld
instances and existing transfer setup; do not replace process-liveness evidence
with a GUnit hook or wall-clock sleep.

- [ ] **Step 2: Add only two values to the existing state machines**

In `sql/preserve_trx.h`, add:

```cpp
enum class Preserve_trx_manager_state {
  // existing values...
  TRANSFER_HANDOFF_COMPLETE
};

enum class Preserve_trx_drain_terminal : uint8_t {
  // existing values...
  TRANSFER_HANDOFF
};
```

Do not add a new state-machine class, registry, public API, sysvar, or protocol
field. `TRANSFER_HANDOFF` is non-restorable. Keep `SHUTDOWN_HANDOFF` for paths
that still close the process.

- [ ] **Step 3: Make FINAL_ACK publish transfer ownership, not shutdown intent**

Change `Preserve_trx_drain_ownership_state::acknowledge_commit()` so
`HANDOFF_PENDING` or `COMMIT_UNKNOWN` transitions to `TRANSFER_HANDOFF`.
Update `restore_allowed()` to return false for both `TRANSFER_HANDOFF` and
`SHUTDOWN_HANDOFF`.

The `RECEIVER_FENCED_SOURCE_OWNS` COMMIT-unknown resolution continues to restore
the source and return the manager to `IDLE`. The
`RECEIVER_OWNS_SOURCE_FENCED` resolution must enter
`TRANSFER_HANDOFF_COMPLETE`; it must not enter `SHUTDOWN_REQUESTED`.

- [ ] **Step 4: Split the successful DRAIN tail by artifact mode**

Rename or split `finish_with_shutdown()` so common work remains common:

```text
publish metrics
audit success
finalize drain participants
```

Then branch:

```text
STANDBY_TRANSFER_SAVE:
  require ownership == TRANSFER_HANDOFF
  manager -> TRANSFER_HANDOFF_COMPLETE with owner_thread_id=0
  dismiss manager guard
  send structured transfer DRAIN result
  return DRAIN success

LOCAL_CARRIER/non-transfer:
  manager -> SHUTDOWN_REQUESTED
  shutdown(thd, SHUTDOWN_DEFAULT, ...)
```

The strict transfer branch must not call `shutdown()`, `kill_mysql()`,
`preserved_trx_defer_shutdown_signal()`, or
`cleanup_after_failed_shutdown()`. A DRAIN result transport failure after
FINAL_ACK only logs the existing ownership-aware warning; the manager remains
`TRANSFER_HANDOFF_COMPLETE` and source transactions must not be restored.

- [ ] **Step 5: Keep the live source fenced**

Treat `TRANSFER_HANDOFF_COMPLETE` as active in the existing CLOSING
default-deny command gate. Preserve these rules:

```text
PRESERVED_DRAINED source THDs remain blocked
ordinary new source commands are blocked with existing 4020 behavior
HA control connection retains its existing bypass for diagnostics/SHUTDOWN
manager never returns automatically to IDLE
```

Do not rollback, reactivate, detach-destroy, or expire transferred source
transactions after FINAL_ACK. Only source epoch session, frame sink, warmcopy
temporary objects, and other ownership-neutral transport resources may be
released. Locks, Undo, and detached transaction objects remain until HA
explicitly shuts down or rebuilds the old source.

For initial zero-target or safely all-excluded transfer, there is no FINAL_ACK
and no accepted receiver epoch. Complete the same current-process manager
fence through an explicit empty-handoff transition from `RUNNING`; do not
fabricate a COMMIT/ACK. Ambiguous exclusion still fails instead of entering
this terminal.

- [ ] **Step 6: Add GUnit state and gate coverage**

In `preserve_trx-t.cc`, cover:

```text
acknowledge_commit produces TRANSFER_HANDOFF
TRANSFER_HANDOFF is not restorable
shutdown_without_commit still produces SHUTDOWN_HANDOFF
TRANSFER_HANDOFF_COMPLETE is default-deny for ordinary commands
HA control connection remains allowed
preserved_trx_shutdown_requested is false for TRANSFER_HANDOFF_COMPLETE
```

Do not expose a new production test interface. Exercise existing public/internal
state and command-gate surfaces or use Debug-only `DEBUG_SYNC` in MTR.

- [ ] **Step 7: Update the real transfer harness**

In `resumable_trx_business_e2e.py`:

1. For transfer scenarios, join the DRAIN operation and validate its structured
   result instead of waiting for DRAIN-triggered source shutdown.
2. Verify the source remains reachable through the HA control connection and
   ordinary business work remains fenced.
3. Capture FINAL_ACK and receiver accepted/READY evidence.
4. Only when the scenario needs source process termination for physical copy or
   teardown, issue a separate HA-controlled `SHUTDOWN` after those assertions.
5. Keep local shutdown/startup scenarios on the existing
   `wait_until_down()` path.

Extend the Python harness unit tests to prove that transfer scenarios never
infer DRAIN success from a disconnect and that explicit HA shutdown occurs
after DRAIN-result/receiver evidence.

- [ ] **Step 8: Run the transfer-terminal slice**

Run:

```bash
cmake --build build-debug --target mysqld preserve_trx-t -j8
./build-debug/runtime_output_directory/preserve_trx-t \
  --gtest_filter='*TransferHandoff*:*ShutdownHandoff*:*CommandGate*'
perl build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --do-test=standby_transfer_drain_no_shutdown \
  --parallel=1 --force
perl build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --do-test=standby_transfer_drain_no_shutdown \
  --mysqld=--log-bin=mysql-bin --parallel=1 --force
python3 -m unittest scripts.tests.test_resumable_trx_business_e2e
```

Expected:

```text
strict transfer DRAIN returns before any explicit HA shutdown
source stays alive and fenced
explicit HA shutdown succeeds afterward
local carrier shutdown contracts remain green
```

- [ ] **Step 9: Pass the mandatory slice review gate**

The correctness reviewer must inspect FINAL_ACK ownership, COMMIT-unknown
resolution, command fencing, manager-owner clearing, DRAIN-result failure, and
the prohibition on source transaction rollback/destruction. The convergence
reviewer must reject duplicated finish logic, a new public API, any hot-path
hook, or more than 45 net production lines for this slice.

- [ ] **Step 10: Commit the transfer terminal slice**

```bash
git add sql/preserve_trx.h sql/preserve_trx.cc \
  unittest/gunit/preserve_trx-t.cc \
  mysql-test/suite/preserve_trx/t/standby_transfer_drain_no_shutdown.test \
  mysql-test/suite/preserve_trx/r/standby_transfer_drain_no_shutdown.result \
  scripts/resumable_trx_business_e2e.py \
  scripts/tests/test_resumable_trx_business_e2e.py
git commit -m "preserve_trx: keep transfer source fenced without shutdown"
```

---

### Task 3: Publish Receiver Partial READY Selection

**Files:**
- Modify: `sql/preserve_trx.cc`
- Modify: `sql/preserve_trx_transfer.h`
- Modify: `sql/preserve_trx_transfer.cc`
- Modify: `sql/preserve_trx_promotion_prepared.h`
- Modify: `sql/preserve_trx_promotion_prepared.cc`
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Create: `mysql-test/suite/preserve_trx/t/transfer_receiver_partial_ready_selection.test`
- Create: `mysql-test/suite/preserve_trx/r/transfer_receiver_partial_ready_selection.result`

- [ ] **Step 1: Store selection directly in the accepted epoch**

Do not add a separate shared selection object. Add:

```cpp
enum class Preserve_trx_receiver_exclusion_reason : uint8_t {
  PREWARM_DEADLINE_NOT_READY,
  PREWARM_UNSUPPORTED_AFTER_IDENTITY
};

struct Preserve_trx_receiver_excluded_token {
  uint64_t token{0};
  Preserve_trx_receiver_exclusion_reason reason{
      Preserve_trx_receiver_exclusion_reason::PREWARM_DEADLINE_NOT_READY};
};
```

Extend `Preserve_trx_transfer_accepted_epoch`:

```cpp
std::vector<uint64_t> ready_tokens;
std::vector<Preserve_trx_receiver_excluded_token> excluded_tokens;
std::vector<Preserve_trx_resurrection_index_entry>
    authenticated_resurrection_entries;
uint64_t selection_binding_generation{0};
bool selection_published{false};
bool promotion_completed{false};
```

Keep `tokens` as the complete accepted fact set. These fields are process-local
and copied with the accepted epoch lease; do not create a separate selection
object or identity registry.

- [ ] **Step 2: Add selected-key deadline update**

Add an overload to `Preserve_trx_prepared_token_registry`:

```cpp
Preserve_trx_prepared_status update_selected_prepare_deadline(
    const std::vector<Preserve_trx_prepared_token_key> &keys,
    uint64_t deadline_monotonic_us);
```

Implementation requirements:

1. Reject empty keys; zero-survivor skips this helper.
2. Require one epoch scope and epoch id.
3. Sort and reject duplicates.
4. Lock registry only while resolving shared entries.
5. Lock entries in token order.
6. Accept only `READY_FACTS_PENDING_LEASE` or `READY_FOR_GATE`.
7. Rebuild canonical digest exactly as the existing all-epoch helper does.
8. Publish all updated immutable publications only after all allocations and
   validations succeed.

Do not alter `update_epoch_prepare_deadline()`; the current all-ready path must
remain behaviorally unchanged.

- [ ] **Step 3: Authenticate identity at sealed-token admission**

Extract the identity portion of
`resurrection_index_matches_receiver_bundle()` into one production-internal
helper that reads the sealed Resurrection Index and validates the
manifest-bound identity:

```text
Index epoch and token
Preserve XID shape and token binding
source prepare LSN against the manifest
snapshot digest against the snapshot descriptor
trx id, Undo anchors, and modified table ids for structural validity
complete encoded-manifest digest and current receiver process generation binding
```

The sealed Index object digest authenticates the entry values; the later
COMMIT fact authenticates the complete manifest digest. The identity-only
helper must not claim an independent physical proof for trx id or Undo anchors.
Checks that require the decoded snapshot bundle, including
`modified_table_ids.size()` against `metadata.mod_tables_count`, stay in the
existing staged-token path. That path must reuse the cached verified entry
rather than decode the Index again. Identity completion alone never means
`READY_FOR_GATE`.

Use the existing `SEAL_OBJECT`/`all_objects_sealed` admission and existing
receiver ready-state ownership:

1. After `mark_object_sealed()` reports that every token object is sealed,
   capture its manifest digest/process generation, then call the identity
   helper before `enqueue_receiver_staged_token_prewarm()`.
2. Store the verified entry in generation-local `Receiver_epoch_ready_state`
   data, keyed by token and expected manifest identity.
3. On identity failure, do not enqueue the staged heavy job; return the strict
   integrity failure through the existing frame/epoch fail-closed path.
4. After identity succeeds, reacquire the registry lock and require the same
   manifest digest/process generation and still-sealed/non-aborted state before
   storing identity and enqueueing the unchanged heavy staged-token job.
5. When `COMMIT_EPOCH` publishes the exact fact, perform one O(token count)
   pass that binds every manifest identity to the corresponding fact token and
   manifest digest. Do not reread payload or repeat heavy prewarm.
6. ABORT, CORRUPT, and allowed manifest replacement remove only transient
   identity/job state matching the old expected manifest identity.
7. At selection publication, copy the complete sorted fact-bound identity set into
   `accepted.authenticated_resurrection_entries` and clear the transient
   generation-local entries through existing derived-state cleanup.

This hook runs only on the authenticated transfer control path, not the normal
SQL/binlog/InnoDB business hot path. For strict v1 it must require
`transfer_object_uses_strict_v1_memory_staging()` and read the already sealed
payload through
`Preserve_trx_transfer_receiver_registry::read_strict_v1_object()`. Reject a
non-strict manifest; do not call the generic accessor that may fall back to a
staging file. It must not hydrate external blobs, acquire DD/lock leases, or
perform network I/O.

Do not delay or reschedule the existing per-object record-lock/binlog prewarm.
Those jobs may run before all token objects are sealed, but their outputs remain
provisional and cannot make the token READY without the identity-admitted
staged-token path.

Do not add a dedicated identity thread, queue, registry, or new accepted
lifecycle. A token may become receiver-excluded only if its authenticated
entry exists. Missing, duplicate, stale, or inconsistent identity blocks the
whole selection.

- [ ] **Step 4: Add one accepted-registry publication method**

Add:

```cpp
Preserve_trx_transfer_status mark_accepted_epoch_ready_with_selection(
    const Preserve_trx_transfer_accepted_epoch &expected,
    uint64_t now_us,
    uint64_t ready_deadline_monotonic_us,
    const std::vector<uint64_t> &ready_tokens,
    const std::vector<Preserve_trx_receiver_excluded_token> &excluded_tokens,
    const std::vector<Preserve_trx_resurrection_index_entry>
        &authenticated_entries);
```

Under the existing accepted registry mutex validate:

```text
lifecycle == PREWARMING
root/epoch/fact digest/process generation match expected
binding generation still matches the publication attempt
fact and accepted token sets are unchanged
ready and excluded are sorted and unique
ready UNION excluded == accepted.tokens
ready INTERSECT excluded == empty
authenticated entries match every accepted token exactly once
selection was not previously published
```

Then atomically assign the vectors, set `selection_published`, move lifecycle
to `READY`, install the new READY deadline, and notify bootstrap waiters after
releasing the registry mutex.

Retain `mark_accepted_epoch_ready()` as an all-ready wrapper only if it carries
the same expected snapshot and binding generation into the atomic publication.
Do not keep an unlocked query-then-update wrapper.

- [ ] **Step 5: Define cutoff transition semantics**

The new selection publication method, not the old all-ready wrapper, owns the
PREWARMING-cutoff transition:

```text
PREWARMING at cutoff
  -> freeze ready/excluded partition
  -> install a new READY deadline
  -> READY(selection)
```

It may publish at the expired PREWARMING cutoff only when the accepted record
is still `PREWARMING` and the same reaper pass has not changed it to `EXPIRED`.
Once `EXPIRED` is visible, classification must fail closed. This prevents an
ordinary late caller from resurrecting an expired epoch.

- [ ] **Step 6: Reuse the existing receiver READY mutex as the classification linearization point**

At the classification cutoff:

1. Lock `g_receiver_ready_epoch_mutex`.
2. Reject if `binding` or `bound` already describes a conflicting publication.
3. Copy sorted `fact_tokens`, `ready_tokens`, and authenticated entries.
4. Require every fact token, including excluded tokens, to have authenticated
   identity.
5. Mark the epoch binding generation in progress and unlock.
6. Bind only the ready subset using the existing
   `preserved_trx_promotion_bind_prewarmed_epoch_fact_for_receiver()`.
7. Update only selected prepared-token deadlines.
8. Publish accepted selection.
9. On any failure, invoke existing derived-state purge and do not report READY.

No new `PUBLISHING` token state is needed. During binding, the frozen candidate
snapshot may be used. After accepted publication, the accepted epoch is the
only selection authority; clear transient candidate/identity sets.
`note_receiver_epoch_token_ready()`
must reject both in-progress and completed publication:

```cpp
if ((state.binding || state.bound) &&
    frozen_candidate_tokens.count(token) == 0) {
  cleanup_late_excluded_token = true;
} else if (!state.binding && !state.bound) {
  state.ready_tokens.insert(token);
}
```

Perform late-token cleanup after releasing the ready-state mutex. Cleanup must
cover strict prepared entry, staging object, proof, queue/inflight dedupe, and
generation-local identity; it may erase only matching expected identity.

Promotion completion is part of this same Task 3 slice:
`complete_accepted_epoch_promotion_lease()` sets `promotion_completed=true`
and erases only when late-worker/cleanup/proof/dedupe ownership is already
terminal. Otherwise the reaper retires the epoch after those references drain.
Do not defer this invariant to Task 5.

- [ ] **Step 7: Order classification before expiry**

In the global Preserve reaper:

```text
receiver selection cutoff/finalize
selected prepared deadline publication
accepted READY publication
prepared-token expiry
accepted READY/TTL expiry
staging cleanup retry
worker idle-stop check
```

The selected-key deadline update must happen before prepared entries can be
expired in the same reaper pass.

- [ ] **Step 8: Add deterministic GUnit races**

Add tests using barriers/fake monotonic time:

```text
990 ready + 10 excluded produces an exact immutable partition
0 ready + N excluded is a valid selection
more tokens than workers authenticate at sealed admission while every heavy worker is blocked
missing resurrection identity blocks selection
corrupt/duplicate identity fails the epoch instead of becoming excluded
zero-survivor publication does not update selected prepare deadlines
internal selection READY still queries as COMMITTED_NOT_READY
late worker after selection cannot expand ready_tokens
selection publication before same-timestamp prepared expiry keeps selected resources
repeated publication is idempotent only for the identical selection
late worker during binding cannot publish or leak derived state
accepted completion does not erase the epoch before cleanup reaches terminal
```

Do not use wall-clock sleeps.

- [ ] **Step 9: Add the MTR**

`transfer_receiver_partial_ready_selection.test` must:

1. Configure two receiver prewarm workers and transfer four real tokens.
2. Allow token A to finish strict prewarm and become the known READY survivor.
3. Use Debug `DEBUG_SYNC`, not wall-clock sleep or a production test API, to
   occupy both heavy workers with tokens B and C after their identity admission.
4. Seal token D while both heavy workers remain blocked and assert its identity
   admission completes synchronously without waiting for either worker.
5. Reach receiver cutoff.
6. Assert accepted lifecycle is READY with exactly `{A}` READY and `{B,C,D}`
   RECEIVER_EXCLUDED.
7. Release the blocked workers.
8. Assert the published selection does not change.
9. Assert excluded staging/prepared resources eventually reach zero.

- [ ] **Step 10: Run the receiver slice**

Run:

```bash
cmake --build build-debug --target preserve_trx-t mysqld -j8
./build-debug/runtime_output_directory/preserve_trx-t --gtest_filter='*PartialReady*:*Selection*:*PreparedDeadline*'
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test=transfer_receiver_partial_ready_selection --parallel=1 --force
```

Expected: GUnit and no-bin MTR pass.

Repeat the MTR with:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test=transfer_receiver_partial_ready_selection --mysqld=--log-bin=mysql-bin --parallel=1 --force
```

- [ ] **Step 11: Pass the mandatory slice review gate**

Freeze the Task 3 diff and evidence. The correctness reviewer must focus on
selection publication, late-worker cleanup, deadline/expiry ordering, and
identity completeness. The convergence reviewer must check receiver hot paths,
OFF-path isolation, prepared-registry reuse, and ACK-to-READY/prewarm metrics.
It must also prove that the sealed-admission helper releases registry locks
before decode, reads only the bounded in-memory strict Index, leaves existing
per-object prewarm scheduling unchanged, and causes no unexplained frame-ACK or
FINAL_ACK regression.
Resolve and reverify every blocking finding.

- [ ] **Step 12: Commit the receiver selection slice**

```bash
git add sql/preserve_trx.cc sql/preserve_trx_transfer.h \
  sql/preserve_trx_transfer.cc \
  sql/preserve_trx_promotion_prepared.h \
  sql/preserve_trx_promotion_prepared.cc \
  unittest/gunit/preserve_trx-t.cc \
  mysql-test/suite/preserve_trx/t/transfer_receiver_partial_ready_selection.test \
  mysql-test/suite/preserve_trx/r/transfer_receiver_partial_ready_selection.result
git commit -m "preserve_trx: publish partial receiver ready selection"
```

---

### Task 4: Consume the READY Subset and Hand Excluded Transactions to Native Rollback

**Files:**
- Modify: `sql/preserve_trx_transfer.h`
- Modify: `sql/preserve_trx_transfer.cc`
- Modify: `sql/preserve_trx_promotion.h`
- Modify: `sql/preserve_trx_promotion.cc`
- Modify: `storage/innobase/include/trx0preserve.h`
- Modify: `storage/innobase/trx/trx0preserve.cc`
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Modify: `unittest/gunit/innodb/trx0preserve-t.cc`

- [ ] **Step 1: Preserve both sets in the bootstrap attempt**

Extend `Preserve_trx_physical_promotion_bootstrap_attempt::Impl` with:

```cpp
std::vector<Preserve_trx_prepared_token_key> ready_keys;
std::vector<trx_preserve_resurrection_facts> receiver_excluded_facts;
```

Do not expose verified `trx_t *` outside the existing attempt/gate boundary.

- [ ] **Step 2: Wait for selection without changing global deadlines**

Add an internal wait/acquire helper backed by the accepted registry condition
variable. It waits until selection publication, epoch terminal failure, or the
single operation deadline. It must not poll, sleep, publish selection, move the
PREWARMING cutoff, or extend token/epoch TTL.

Convert the caller's wall deadline to one absolute monotonic deadline once.
After wakeup, acquire the existing accepted promotion lease only if the same
fact digest, process generation, and published selection remain visible.
PREWARM cutoff produces the partial READY selection; an earlier bootstrap
operation timeout does not snapshot a still-changing subset and therefore
returns no Preserve attempt for that call.

- [ ] **Step 3: Register candidates for READY tokens only**

`preserved_trx_prepare_before_trx_sys_init_for_physical_promotion()` must:

1. Wait for and acquire the accepted selection.
2. Verify selection partitions the unchanged fact.
3. Build `ready_keys` only for `ready_tokens`.
4. Read accepted-lease authenticated entries for every fact token.
5. Register only READY entries through the existing
   `trx_preserve_startup_register_resurrection_candidate()` path.
6. Copy excluded entries into `receiver_excluded_facts`; do not register them as
   candidates.
7. Validate every fact `prepare_lsn` as nonzero and no later than the accepted
   source fence.
8. Update and pin strict prepared resources only for `ready_keys`.
9. Skip candidate registration, selected-deadline update, and prepared pin when
   `ready_keys` is empty.

If an excluded token has no authenticated resurrection entry, abort the
Preserve bootstrap attempt; do not guess its `trx_t`.

This is the only place that changes the full-epoch request construction. Keep
the current full fact checks:

```text
accepted.tokens == accepted.fact.tokens
ready_tokens UNION excluded_tokens == accepted.tokens
ready_tokens INTERSECT excluded_tokens == empty
```

Then build the gate input from the authenticated subset:

```text
gate_request.tokens == ready_keys
gate_request.tokens SUBSET_OF accepted.fact.tokens
```

Do not weaken or remove the accepted fact validation to make the subset pass.
Do not create a replacement epoch fact or subset fact digest.

- [ ] **Step 4: Reuse accepted-lease resurrection identity**

Read each authenticated entry directly from the acquired accepted epoch. If a
small copy helper improves encapsulation, it must consume the accepted snapshot
rather than perform a second registry lookup:

```cpp
bool preserve_trx_transfer_copy_accepted_resurrection_entries(
    const Preserve_trx_transfer_accepted_epoch &accepted,
    const std::vector<uint64_t> &tokens,
    std::vector<Preserve_trx_resurrection_index_entry> *entries);
```

It must:

1. Read only process-local entries already authenticated at selection
   publication.
2. Return entries in the exact sorted token order requested.
3. Reject missing, duplicate, or snapshot-mismatched tokens.
4. Never read an artifact file during promotion bootstrap.

The sealed-token admission captures identity once; promotion reuses it. Do not
decode the same Resurrection Index a second time.

- [ ] **Step 5: Add one-scan batch InnoDB handoff helper**

Declare in `trx0preserve.h`:

```cpp
dberr_t trx_preserve_release_recovered_prepared_batch_to_native_rollback(
    const std::vector<trx_preserve_resurrection_facts> &excluded);
```

Implementation in `trx0preserve.cc`:

1. Reject empty/invalid facts, zero `prepare_lsn`, and duplicate token/XID/trx
   id. The caller has already checked each prepare LSN against the accepted
   source fence.
2. Under `trx_sys` mutex scan `rw_trx_list` once and map only Preserve-magic
   recovered PREPARED transactions by XID/trx id.
3. Compare each matched transaction's Preserve-magic XID and trx id with the
   authenticated facts. A non-candidate recovered `trx_t` does not contain
   `preserve_prepare_lsn`; do not invent a second actual-LSN check here. Do not
   require Undo-anchor equality: anchor mismatch is a valid reason for a READY
   candidate to fall back to full Undo scan and native rollback.
4. Validate every matched transaction before any mutation:

   ```text
   is_recovered == true
   state == TRX_STATE_PREPARED
   mysql_thd == nullptr
   preserve magic XID
   preserve_trx_claimed == false
   ddl_operation == false
   transaction is not dictionary/DD recovery owned
   every non-null Undo header state == TRX_UNDO_PREPARED
   ```

5. If any lookup or validation fails, return `DB_ERROR` without changing any
   state.
6. Under the same mutex decrement `n_prepared_trx` for each transaction and
   store `TRX_STATE_ACTIVE`.
7. Reuse the existing Undo activation primitive under its required lock
   discipline and leave `is_recovered=true`.

The current Undo activation primitive has no recoverable failure after valid
preconditions. This gives no-partial-return behavior in the current process,
not crash atomicity. A crash during conversion invalidates the process-local
receiver selection and must not permit write-enable. If implementation
introduces any operation that may fail after the first transaction mutation,
stop Task 4 and redesign batch atomicity. Do not rely on a Debug/Release
assertion to make a fallible partial conversion safe.

Do not run transaction rollback inside this helper.

- [ ] **Step 6: Partition once after `trx_sys` initialization**

In `prepare_gate_handoff()`:

1. Resolve READY tokens once with
   `trx_preserve_startup_resurrection_find_verified()`.
2. A READY candidate that fell back or has no verified pointer moves only into
   the attempt-local excluded facts; the receiver classification remains
   immutable.
3. Require pointer and token uniqueness.
4. Call the new batch InnoDB handoff once for preclassified excluded facts plus
   READY fallback facts.
5. Put only final READY verified pointers in the existing
   `verified_transactions` vector.
6. Complete the batch handoff before the caller's one-time
   `trx_sys_need_rollback()` evaluation. After successful handoff, an excluded
   non-empty batch must make that evaluation return true.

No `rw_trx_list` scan per token is allowed.

The first-release physical-standby call-site contract is:

```text
trx_sys_init_at_db_start()
  -> prepare_gate_handoff()
  -> srv_dict_recover_on_restart()/dictionary cleanup
  -> trx_sys_need_rollback()
  -> start recovery rollback worker when needed
  -> DD/MDL-ready strict adopt
  -> purge/write-enable
```

In the native startup ordering, the preferred insertion point is immediately
after `trx_sys_init_at_db_start()` and before
`srv_dict_recover_on_restart()`/
`trx_rollback_or_clean_recovered(FALSE)`. This prevents an excluded DDL
transaction from missing dictionary recovery; the batch helper also
defensively rejects `ddl_operation`/dictionary-owned transactions. If an
external integration can only call after dictionary cleanup, it must first
prove the accepted selection contains no such transaction.

If the external promotion architecture has already passed that sampling point,
stop Task 4 integration. Do not call `srv_start_threads()` again and do not
start a thread inside the batch handoff helper. A narrow idempotent
recovery-rollback-worker ensure API requires a separate design review.

- [ ] **Step 7: Handle zero READY survivors explicitly**

When `ready_keys.empty()`:

1. Confirm Task 3 skipped the non-empty selected-deadline helper.
2. Do not acquire a prepared-token promotion pin.
3. Complete excluded native handoff.
4. Complete the accepted receiver promotion lease.
5. Release the bootstrap attempt.
6. Return `OK` with `adopted_count == 0`.
7. Do not call the existing strict gate that rejects an empty token vector.

This means ordinary physical promotion continues while no Preserve SQL RESUME
token is exposed.

- [ ] **Step 8: Keep strict subset all-or-nothing**

For non-empty READY survivors, call the existing strict adopt implementation
with only READY keys and verified pointers. Any strict survivor failure still
reverses/fails the complete READY subset; do not add per-token partial success
inside the strict gate.

The strict gate is compensating fail-closed, not crash-atomic: success means
the complete READY subset adopted; a failed reversal may leave
`CLEANUP_TAINTED`. That status permanently blocks this promotion's
write-enable and must not be converted into ordinary-success fallback.

Before that call, replace the attempt's original all-fact gate vector with the
ordered final-survivor vector produced in Step 6. Assert:

```text
gate_request.tokens.size() == verified_transactions.size()
every gate key belongs to immutable selection.ready
no duplicate token or trx_t pointer
```

The strict executor already iterates the explicit request vector and validates
each prepared snapshot against the original accepted epoch. Do not modify its
worker scheduling, intent, digest-input construction, adopt, or reversal
algorithms for this feature. The only prepared-registry addition is the
selected-key deadline helper from Task 3; reuse the existing explicit-key pin.

- [ ] **Step 9: Add InnoDB GUnit coverage**

In `trx0preserve-t.cc`, cover:

```text
two authenticated facts are matched in one scan and transition to ACTIVE
native XA PREPARED is rejected with no mutation
duplicate token/XID/trx id is rejected with no mutation
claimed transaction is rejected
mixed valid/invalid batch changes none
anchor mismatch still hands the exact XID/trx-id transaction to native rollback
zero-size batch is skipped by the SQL coordinator, not accepted by the helper
```

- [ ] **Step 10: Add promotion GUnit coverage**

In `preserve_trx-t.cc`, cover:

```text
bounded wait wakes on selection and times out without changing global deadline
all-ready selection follows the existing strict path
partial selection adopts only READY verified pointers
only READY entries are registered as Resurrection candidates
excluded facts are handed off once after native Undo-body scan
READY anchor fallback joins the same batch handoff
successful non-empty handoff makes trx_sys_need_rollback true before sampling
zero-survivor selection skips strict gate and completes lease
zero-survivor selection acquires no prepared pin and changes no selected deadline
missing excluded fact lookup fails before write-enable
strict READY subset failure preserves current all-or-nothing reversal
```

- [ ] **Step 11: Add simulator evidence without a fake physical MTR**

Extend the existing promotion simulator/GUnit surface to verify:

```text
selection acquisition
READY-only candidate registration
READY subset construction
excluded handoff state
zero-survivor no-op gate
existing SQL RESUME path for an adopted survivor
```

The test result and plan must not claim real physical redo-apply/write-enable
E2E. That remains an external physical-standby acceptance gate.

- [ ] **Step 12: Run the promotion slice**

Run:

```bash
cmake --build build-debug --target preserve_trx-t trx0preserve-t mysqld -j8
./build-debug/runtime_output_directory/trx0preserve-t
./build-debug/runtime_output_directory/preserve_trx-t --gtest_filter='*PhysicalPromotion*:*PartialReady*'
```

Expected: all selected tests pass.

- [ ] **Step 13: Pass the mandatory slice review gate**

Freeze the Task 4 diff and evidence. Run the two standard reviewers plus the
InnoDB recovery reviewer. Require explicit source-backed approval of the
single-scan lookup, PREPARED-to-ACTIVE/Undo activation, strict-subset atomicity,
rollback/purge/write-enable ordering, and zero-survivor behavior. Rerun all
affected GUnit/simulator tests after any correction.

- [ ] **Step 14: Commit the promotion/handoff slice**

```bash
git add sql/preserve_trx_transfer.h sql/preserve_trx_transfer.cc \
  sql/preserve_trx_promotion.h sql/preserve_trx_promotion.cc \
  storage/innobase/include/trx0preserve.h \
  storage/innobase/trx/trx0preserve.cc \
  unittest/gunit/preserve_trx-t.cc \
  unittest/gunit/innodb/trx0preserve-t.cc
git commit -m "preserve_trx: adopt ready subset during physical promotion"
```

---

### Task 5: Stop Idle Receiver Workers Without Destroying READY Resources

**Files:**
- Modify: `sql/preserve_trx.cc`
- Modify: `sql/preserve_trx_transfer.h`
- Modify: `sql/preserve_trx_transfer.cc`
- Modify: `sql/mysqld.cc`
- Modify: `unittest/gunit/preserve_trx-t.cc`
- Create: `mysql-test/suite/preserve_trx/t/transfer_receiver_worker_auto_stop_restart.test`
- Create: `mysql-test/suite/preserve_trx/r/transfer_receiver_worker_auto_stop_restart.result`

- [ ] **Step 1: Reuse Task 3 accepted-epoch retirement**

Task 3 already changes completion to set `promotion_completed` and adds
conditional reaper retirement. Task 5 must consume that predicate when deciding
whether worker-owned cleanup is empty; it must not duplicate or move the
accepted-epoch ownership rule.

- [ ] **Step 2: Add one runtime stop request flag**

Reuse existing worker lifecycle booleans and add:

```cpp
bool g_receiver_prewarm_idle_stop_requested = false;
bool g_receiver_prewarm_process_shutdown_started = false;
```

Do not overload `g_receiver_prewarm_shutdown`; full process shutdown and idle
runtime stop have different cleanup semantics. The permanent process-shutdown
latch is set from the real `mysqld.cc` shutdown path before worker shutdown
begins and is never cleared. Lazy restart must reject it.

- [ ] **Step 3: Let workers exit on idle-stop request**

In `receiver_prewarm_worker_main()`:

```cpp
if (g_receiver_prewarm_shutdown) return;
if (g_receiver_prewarm_idle_stop_requested &&
    g_receiver_staged_token_prewarm_jobs.empty() &&
    g_receiver_prewarm_jobs.empty()) {
  return;
}
```

The predicate must be evaluated under `g_receiver_prewarm_mutex`.
Evaluate it before the paused and profile-limit branches and again after every
condition-variable wakeup. Every predicate wait must include
`g_receiver_prewarm_idle_stop_requested`; in particular, the paused wait cannot
wait only for shutdown/unpause. The existing profile-limited
`wait_for(100ms)` may remain, but `notify_all()` must return it to the top-level
idle-stop check rather than another profile wait.

- [ ] **Step 4: Define worker-owned work directly**

Add one internal, lock-protected predicate:

```cpp
bool receiver_prewarm_has_unfinished_worker_owned_work_locked();
```

It checks:

```text
staged-token and object queues
active worker count and active-by-registry map
staged/object inflight sets
deferred staged/record-plan work
enqueue admission refcount
worker-owned proof/finalizer/cleanup references
```

Do not use `m_online_epochs.size()`: online epochs also retain terminal protocol
state and do not imply runnable worker work. Do not maintain an unprotected
global list of registry pointers. Custom/non-singleton registry destruction
must first retire its queued/active jobs through the existing registry
retirement barrier.

Enqueue admission must be acquired under `g_receiver_prewarm_mutex` before the
first registry dereference. Registry destruction permanently closes admission,
then waits for queued, active, and admission counts to reach zero before
returning. The deterministic race test must pause after admission acquisition
but before the first registry lookup; no pointer may outlive that barrier.

- [ ] **Step 5: Add a join-only stop helper**

Implement:

```cpp
bool stop_idle_receiver_prewarm_workers_if_possible();
```

Under `g_receiver_prewarm_mutex`, return false when:

```text
workers not started
workers starting or stopping
queued jobs are non-empty
active worker count is non-zero
active registry admission is non-zero
worker-owned unfinished work exists
```

On success:

1. Set `stopping` and `idle_stop_requested`.
2. Move the worker vector locally.
3. Notify workers.
4. Unlock and join every worker.
5. Relock, set worker count to zero, clear `started/stopping/idle_stop_requested`.
6. Notify waiters.

Queue/inflight/deferred structures must already be empty before stop; do not
silently clear them to make the predicate pass. Do not clear ready state,
prepared registry, accepted classification/fact, cleanup debt, or TTL object.

- [ ] **Step 6: Preserve lazy restart**

`ensure_receiver_prewarm_workers_locked()` must wait while idle stop is in
progress and then start one new pool. After every condition-variable wait,
recheck full shutdown and registry retirement before enqueueing the job.

Move dedupe/already-complete checks before worker startup so a duplicate late
job does not restart an idle pool only to be discarded. Full process shutdown
latch always wins over lazy restart.

- [ ] **Step 7: Call auto-stop from the existing reaper**

Call the helper after:

```text
selection finalize and selected deadline publication
prepared expiry and accepted TTL expiry
cleanup-debt retry
conditional accepted-epoch retirement
queue cleanup
```

The current reaper interval defines stop latency; do not add a new timer or
sysvar.

- [ ] **Step 8: Add deterministic GUnit coverage**

Cover:

```text
READY selection survives worker join
prepared-token registry survives worker join
retained terminal online epoch does not prevent stop when no worker work remains
queued/inflight/deferred/admission work each prevents stop
STOPPING plus new enqueue produces exactly one restarted pool
full shutdown racing idle-stop has one join owner
full shutdown latch prevents restart
duplicate completed job does not restart workers
worker count becomes zero only after join completes
paused worker observes idle-stop and joins without requiring unpause
profile-limited worker observes notify and joins without a 100 ms polling delay
TTL expiry destroys a never-promoted epoch after workers have stopped
promotion lease remains acquirable after workers have stopped
custom registry retirement/destruction leaves no dangling worker reference
```

Use barriers; do not use sleep.

- [ ] **Step 9: Add the MTR**

`transfer_receiver_worker_auto_stop_restart.test` must:

1. Complete a receiver epoch to READY.
2. Wait on status counters until worker count and active count are zero.
3. Verify READY selection remains queryable.
4. Start a second epoch.
5. Verify workers restart and process it.
6. Complete the second epoch and verify workers stop again.

- [ ] **Step 10: Run the worker slice**

Run:

```bash
cmake --build build-debug --target preserve_trx-t mysqld -j8
./build-debug/runtime_output_directory/preserve_trx-t --gtest_filter='*ReceiverWorker*:*AutoStop*'
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test=transfer_receiver_worker_auto_stop_restart --parallel=1 --force
```

Expected: worker stop/restart tests pass with no leaked mysqld or test process.

- [ ] **Step 11: Pass the mandatory slice review gate**

Freeze the Task 5 diff and evidence. The correctness reviewer must verify one
join owner, stop/restart/shutdown races, TTL after stop, and cleanup ownership.
The convergence reviewer must prove that retained terminal epochs do not keep
workers alive, READY resources survive join, no second worker state machine was
added, and worker/prewarm metrics did not regress.

- [ ] **Step 12: Commit the worker slice**

```bash
git add sql/preserve_trx.cc sql/preserve_trx_transfer.h \
  sql/preserve_trx_transfer.cc \
  unittest/gunit/preserve_trx-t.cc \
  mysql-test/suite/preserve_trx/t/transfer_receiver_worker_auto_stop_restart.test \
  mysql-test/suite/preserve_trx/r/transfer_receiver_worker_auto_stop_restart.result
git commit -m "preserve_trx: stop idle receiver prewarm workers"
```

---

### Task 6: Minimal Observability and Contract Audit

**Files:**
- Modify only if required: `sql/preserve_trx_resource.h`
- Modify only if required: `sql/preserve_trx_resource.cc`
- Modify only if required: `sql/mysqld.cc`
- Modify: `mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_lint.test`
- Modify: `mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_lint.result`

- [ ] **Step 1: Reuse result/status surfaces first**

Use:

```text
DRAIN survivor/excluded result rows
promotion gate candidate/excluded/fallback/adopted counts
existing worker count/active/queued
accepted lifecycle, cleanup debt, and prepared-token status
```

Only if no existing value can show worker auto-stop occurrences may one
aggregate stop counter be added through existing STATUS plumbing. Do not add a
receiver runtime-state metric or one counter per reason/stage.

Do not add global READY/EXCLUDED counters as an HA pre-promotion decision
surface. The epoch-scoped counts are returned by the existing promotion gate
result after it acquires the immutable selection. Keep strict
`QUERY_EPOCH_STATUS` limited to current-process commit/ACK ambiguity.

- [ ] **Step 2: Add one publication log and one failure log**

Successful publication:

```text
PRESERVE: receiver selection epoch=<id> ready=<n> excluded=<n>
```

Failure:

```text
PRESERVE: receiver selection rejected epoch=<id> stage=<stage> status=<status>
```

Do not print a line per token; include a bounded reason summary.

- [ ] **Step 3: Audit the implemented diff against Task 0**

The source-shape lint and manual audit must prove:

```text
no separate selection shared_ptr
no separate receiver runtime-state enum
only TRANSFER_HANDOFF and TRANSFER_HANDOFF_COMPLETE are added to existing state machines
no RETIRING lifecycle
no accepted_instance_id
pre-final source exclusion only
strict transfer FINAL_ACK returns DRAIN result without source shutdown
TRANSFER_HANDOFF_COMPLETE remains fenced and never restores transferred trx
LOCAL_CARRIER retains the existing shutdown path
authenticated identity required before receiver exclusion
READY-only Resurrection candidates
single-scan excluded native handoff
join-only worker stop
external physical HA E2E remains out of repository scope
production LOC target 430-600, hard stop at 620
```

- [ ] **Step 4: Run source-shape and status tests**

Run:

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --do-test=code_review_resumable_trx_slices_lint --parallel=1 --force
```

Expected: PASS.

- [ ] **Step 5: Apply the review gate if production code changed**

If this task modified `sql/preserve_trx_resource.*`, `sql/mysqld.cc`, or any
other production file, run the two standard read-only reviews before commit.
The convergence reviewer must specifically check status-path overhead, native
startup/OFF-path isolation, and whether the new metric or log can be removed in
favor of an existing surface. If this task changed only documentation and lint
tests, record that fact and do not dispatch a redundant review batch.

- [ ] **Step 6: Commit observability and documentation**

```bash
git add mysql-test/suite/preserve_trx/t/code_review_resumable_trx_slices_lint.test \
  mysql-test/suite/preserve_trx/r/code_review_resumable_trx_slices_lint.result
# Add sql/preserve_trx_resource.* and sql/mysqld.cc only if Step 1 proved
# existing status plumbing insufficient.
git commit -m "test: enforce partial ready implementation boundaries"
```

---

### Task 7: Full Verification and Acceptance

**Files:**
- Modify only when a verified product defect is found.
- Do not weaken thresholds or assertions to obtain a pass.

- [ ] **Step 1: Build Debug and Release**

```bash
cmake --build build-debug --target mysqld preserve_trx-t \
  preserve_trx_drain-t preserve_trx_warmcopy-t \
  preserve_trx_lock_warmcopy-t preserve_trx_temp_table-t \
  trx0preserve-t lock0warmcopy-t -j8
cmake --build build-release --target mysqld preserve_trx-t \
  preserve_trx_drain-t preserve_trx_warmcopy-t \
  preserve_trx_lock_warmcopy-t preserve_trx_temp_table-t \
  trx0preserve-t lock0warmcopy-t -j8
```

Expected: both builds succeed. If Release GUnit has an existing NDEBUG-only
test compilation issue, fix only the test conditional compilation; do not
expose a production testing interface.

- [ ] **Step 2: Run full Preserve GUnit**

```bash
./build-debug/runtime_output_directory/preserve_trx-t
./build-debug/runtime_output_directory/preserve_trx_drain-t
./build-debug/runtime_output_directory/preserve_trx_warmcopy-t
./build-debug/runtime_output_directory/preserve_trx_lock_warmcopy-t
./build-debug/runtime_output_directory/preserve_trx_temp_table-t
./build-debug/runtime_output_directory/trx0preserve-t
./build-debug/runtime_output_directory/lock0warmcopy-t
./build-release/runtime_output_directory/preserve_trx-t
./build-release/runtime_output_directory/preserve_trx_drain-t
./build-release/runtime_output_directory/preserve_trx_warmcopy-t
./build-release/runtime_output_directory/preserve_trx_lock_warmcopy-t
./build-release/runtime_output_directory/preserve_trx_temp_table-t
./build-release/runtime_output_directory/trx0preserve-t
./build-release/runtime_output_directory/lock0warmcopy-t
```

Expected: all built tests pass with zero unexpected skips.

- [ ] **Step 3: Run full Debug no-bin and log-bin MTR**

```bash
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --parallel=8 --force
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --mysqld=--log-bin=mysql-bin --parallel=8 --force
```

Expected: all suite tests pass. Existing explicit future-capability skips must
be listed separately and not counted as passes.

- [ ] **Step 4: Run full Release no-bin and log-bin MTR**

```bash
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --parallel=8 --force
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --mysqld=--log-bin=mysql-bin --parallel=8 --force
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --parallel=8 --force
perl build-debug/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --mysqld=--log-bin=mysql-bin --parallel=8 --force
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --parallel=8 --force
perl build-release/mysql-test/mysql-test-run.pl --suite=preserve_trx --big-test --mysqld=--log-bin=mysql-bin --parallel=8 --force
```

Expected: regular and `--big-test` suite matrices pass. Explicit future-only
skips are reported separately.

- [ ] **Step 5: Run the standard Preserve regression profile**

```bash
python3 /Users/a1234/.codex/skills/preserve-resume-regression/scripts/preserve_resume_regression.py --profile standard --parallel 8
```

Expected:

```text
Python harness unit tests pass
targeted no-bin/log-bin pass
full no-bin/log-bin pass
no residual mysqld/mysqltest process
```

- [ ] **Step 6: Run Release transfer full-pressure three independent times**

Run three separate invocations with unique runner-generated run ids:

```bash
python3 scripts/preserve_trx_full_pressure_runner.py --profile full --evidence transfer-phase2
```

Acceptance for all three runs:

```text
existing phase2 threshold passes
FINAL_ACK succeeds
source remains alive and fenced after DRAIN result
test-owned source shutdown is issued explicitly by the HA control path
ACK-to-READY threshold passes
selection build/publish is reported and shows no material regression
worker stop occurs only after READY
no threshold is relaxed
```

- [ ] **Step 7: Run Release mixed-transfer full-pressure**

```bash
python3 scripts/preserve_trx_full_pressure_runner.py --profile full --evidence mixed-transfer --required-free-gib 25
```

Acceptance:

```text
source post-command tail is reported honestly
FINAL_ACK-to-READY remains within the existing gate
source remains alive/default-deny until explicit HA shutdown
READY/excluded partition is exact
SQL RESUME for READY survivors remains <= existing 100 ms gate
no business hot-path QPS/P99 regression beyond existing limits
```

- [ ] **Step 8: Run mixed shutdown/startup compatibility**

```bash
python3 scripts/preserve_trx_full_pressure_runner.py --profile full --evidence mixed-shutdown-startup --required-free-gib 25
```

Expected: local durable startup/resume behavior is unchanged. Receiver selection
and auto-stop code must not run on this path. Its DRAIN-triggered shutdown
behavior remains unchanged.

- [ ] **Step 9: Verify OFF-path isolation**

Run the existing OFF-path lint and behavior cases and verify:

```text
preserve_trx_enable=OFF does not enter new source exclusion logic
no receiver selection is allocated
no receiver workers start
native MySQL transaction behavior is unchanged
```

- [ ] **Step 10: Record the external physical-standby acceptance gate**

Before production enablement, the physical-standby project must run an E2E that
proves:

```text
redo reaches source fence and freezes
strict transfer DRAIN returns without shutdown and leaves source default-deny
HA independently removes source traffic and owns any later source shutdown/rebuild
prepare-before-trx_sys is called before the one legal trx_sys initialization
receiver-excluded handoff completes before the one-time trx_sys_need_rollback evaluation
that evaluation starts native rollback when the excluded set is non-empty
READY survivors pass strict adopt
write-enable occurs only after handoff/adopt succeeds
the existing SQL RESUME continues DML and COMMIT on the adopted survivor
```

This repository's simulator, MTR, and GUnit results must not be labeled as that
external production E2E.

- [ ] **Step 11: Run the final independent integration review**

After all verification above passes, freeze the complete feature diff and test
evidence. Run three independent read-only reviews:

```text
source exclusion + receiver publication/reaper
physical bootstrap + InnoDB recovery handoff + strict adopt
scope convergence + OFF-path/native-path isolation + performance evidence
```

The main session must resolve every High/Medium finding and rerun the affected
verification. Do not treat reviewer consensus as a substitute for source or
runtime evidence.

- [ ] **Step 12: Final diff audit**

Run:

```bash
git diff --check
git status --short -uall
git diff --stat
```

Acceptance:

```text
production net additions remain within the 430-600 target and never exceed 620
no wire/SQL/sysvar/error-code change
no *_for_unit_test production API
no unrelated tracked or untracked file is staged
```

## 4. Stop Conditions

Stop implementation and return to design review when any of these occurs:

1. A third source failure phase needs special-case token-local handling.
2. A finalized or already-sent token must be excluded to pass a test.
3. Receiver exclusion is requested without authenticated resurrection identity.
4. Identity authentication still depends on an available heavy worker, or the
   implementation requires a dedicated identity pool/queue/registry.
5. Selection publication requires holding accepted and prepared registry mutexes
   while doing DD, filesystem, network, or worker join operations.
6. Auto-stop requires calling full receiver shutdown cleanup.
7. The transfer no-shutdown slice needs a source TTL, rollback/destructor
   worker, public HA API, or automatic return to `IDLE`.
8. A transferred source transaction is restored, rolled back, or destroyed
   after FINAL_ACK, or ordinary source business commands become allowed.
9. Existing all-ready full-pressure or local startup behavior regresses.
10. Production code exceeds 620 net added lines before external HA integration.

These conditions indicate that the minimal方案 has been exceeded; do not keep
adding compensating states or test-only hooks.

## 5. Recommended Execution Order

```text
Task 0 scheme-2 boundary
  -> Task 1 contract baseline
  -> Task 2 source exclusion
  -> Task 2A transfer terminal without shutdown
  -> Task 3 receiver selection
  -> Task 4 promotion/native handoff
  -> Task 5 worker auto-stop
  -> Task 6 docs/observability
  -> Task 7 full verification
```

Review checkpoint after Tasks 2, 2A, 3, 4, and 5. A slice with no direct
behavioral gain or with disproportionate complexity must be reverted before
continuing.
