# Preserve/Resume Phase2 Command Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. The main session owns all edits and test execution; subagents are read-only reviewers. Do not create commits unless the user later gives explicit permission.

**Goal:** Add the `DEPENDENCY_CONVERGENCE_V1` command-admission policy and the separately approved mode-independent Final-HWM/external-staging source-ordering optimization while preserving transfer/receiver protocol, ownership, and FINAL ACK semantics.

**Architecture:** A new scheduler module owns only T0 command/transaction ledgers, bounded lock-dependency proof, support, permit, command cutoff, HARD, and scheduler-local terminal facts. The existing drain owner in `sql/preserve_trx.cc` continues to own the blocking wait loop, active progress, error classification, CLOSING publication, authoritative targets, preserve, transfer, and receiver lifecycle. Shared SQL/THD/MDL/InnoDB files contain only narrow POD, gate, lifecycle, or immutable-proof hooks.

**Tech Stack:** MySQL 8.0.22 C++17, InnoDB lock system, MySQL MDL, classic protocol dispatch, MTR/DBUG, Python E2E orchestration, existing Preserve source-shape runner, Debug and Release Unix Makefiles builds.

---

## Authority, baseline, and execution rules

- Authoritative design: `Docs/superpowers/specs/2026-08-26-preserve-trx-phase2-command-scheduler-design.md`.
- Implementation baseline: `9c6e6b1f193dcf44ad1dad8f285baabe90aab088`.
- Do not copy code from historical scheduler WIP.
- Do not modify or add Unit/GUnit or Python unit-test files.
- New behavior tests are MTR and the dedicated Python E2E only.
- Do not implement RESET DRAIN concurrency; dependency attempt returns `ER_PRESERVE_TRX_UNSUPPORTED` before `request_reset()`.
- Do not commit, push, stage unrelated files, or change receiver/transfer semantics.
- Run `git status --short -uall` before and after every task. Stop if an unrelated tracked file changes.

Protected baseline hashes follow. The two SQL warmcopy hashes are still the
reference for every existing method and call path; only the exact additive
`refresh_phase1_record_live_fence_for_thread()` declaration/definition named in
design 2.5.1 may differ. The lint must source-shape that method and reject any
other warmcopy hunk.

```text
sql/preserve_trx_transfer.cc              f1766d499c0dee109fc63bf3c675b76779b5a5d647d233598d6d1b53bc1f171f
sql/preserve_trx_transfer.h               ece59049d90ddf65999cc45f194f8460e81bdb74a5c024cf577fa8415639695b
sql/preserve_trx_lock_warmcopy.cc          75a56ba9d8710b1e65f04c54ccc349e3306fc9cf0c741be7c44e61602ab0b533
sql/preserve_trx_lock_warmcopy.h           a1d4ac61b3849ba7dcf7144cf9db29d772f61eabc3efec31ec50ff3ccce23a7d
storage/innobase/include/lock0warmcopy.h    037e4df72473c8c3e112059103f7fdfe92d3d3ad80715c8674dcd78dcbf26d84
storage/innobase/lock/lock0warmcopy.cc      71641cf8cc77095fa45ee0188893e05fe8991600d60967cb8764adc9fb4f8ab0
sql/preserve_trx_drain.cc                  3e4debbd4c1b6cfa3ca6043e4b323f8bacb90f9c337fda3b71e831ea274b83d7
sql/preserve_trx_drain.h                   f9b0d485d338071f657d9c3acc9ed1a4f9546da37d3e48c3f6d2a94bcf44b5f2
unittest/gunit/preserve_trx-t.cc           2572412329284d0920d788dac509a1d6c8ed3d2a05e17e578580f0927e0997bd
```

## File ownership map

| File | Planned responsibility | Estimated changed production LOC |
|---|---|---:|
| `sql/preserve_trx_standby_phase2_scheduler.h` | Scheduler-only types and the narrow owner/gate/lifecycle API | 300 |
| `sql/preserve_trx_standby_phase2_scheduler.cc` | Route lifetime, T0 ledgers, support graph, permit, bounded tick, HARD/abort, immutable summary | 1,780 |
| `sql/preserve_trx.cc` | Mode/source-capture facade, existing owner-loop adapter, command hook forwarding, external-pin teardown composition, final-record capture | 360 |
| `sql/preserve_trx.h` | Narrow facade declarations | 40 |
| `sql/preserve_trx_lock_warmcopy.h/.cc` | One additive candidate-local live-fence setter; no old-path edits | 20 |
| `sql/sql_class.h` | Compact per-THD Phase2 command POD only | 35 |
| `sql/sql_parse.cc` | Packet admission, class-specific BODY commit points, no-response preservation, exact aggregate exit | 35 |
| `sql/sys_vars.cc` | Startup-only scheduler ENUM | 15 |
| `sql/mdl.h` | MDL duration-marker and immutable-proof declarations | 20 |
| `sql/mdl.cc` | MDL wait marker, local PR try-read, immutable wait snapshot, owner-local ticket match | 160 |
| `storage/innobase/include/lock0preserve_plan.h` | Fixed-capacity exact-probe DTO/status declarations | 40 |
| `storage/innobase/include/trx0preserve.h` | Non-allocating THD-to-trx peek/identity declarations | 15 |
| `storage/innobase/trx/trx0preserve.cc` | Non-allocating existing-session lookup and immutable trx identity | 35 |
| `storage/innobase/lock/lock0preserve.cc` | Try-only queue-order exact blocker snapshot | 180 |
| `sql/CMakeLists.txt` | Register the new scheduler source | 5 |
| `mysql-test/suite/preserve_trx/*` | Seven runtime scheduler MTR groups and explicit suite legacy default |
| `scripts/preserve_trx_phase2_scheduler_e2e.py` | Three scheduler-specific two-mysqld scenarios and final-record oracle |
| `scripts/preserve_trx_full_pressure_runner.py` | New dependency profiles that delegate workload execution |
| `scripts/preserve_trx_lint_runner.py` | Allowed-surface and protected-pipeline source guards |

Files that must remain unchanged include `sql/sql_class.cc`, `sql/sql_prepare.cc/.h`, `sql/preserve_trx_drain.*`, all transfer/receiver/promotion implementations, every warmcopy method other than the exact additive candidate setter above, `lock0lock.cc`, `lock0wait.cc`, `row0mysql.cc`, and Unit/GUnit.

Production-source budget: center 3,020 lines, allowed range 2,820–3,220. The table is a responsibility budget, not a quota: do not add scaffolding to hit it. The new scheduler header/source should contain about 2,080 lines and at least 65% of the production change; any per-file drift must still preserve the total range and thin-hook boundaries.

## Current RED closure sequence (2026-08-31)

This section supersedes any earlier task wording that forbids the narrow optimization now defined by design 2.5.1. It does not authorize any CLOSING/post-HARD pipeline change.

1. **Transaction identity RED/GREEN**
   - Extend the existing lineage MTR, not Unit/GUnit, with SQL-active + InnoDB `NOT_STARTED` at T0.
   - RED is scheduler `SAFETY_ABORT`/4013 caused by treating a candidate raw pointer as a sealed SQL-only identity.
   - In `sql/preserve_trx_standby_phase2_scheduler.cc`, keep `Engine NONE` pending; seal only on first `EXACT_ACTIVE`; close only when the SQL old transaction truly ends.
   - Re-run lineage plus InnoDB/MDL scheduler MTR. No receiver or transfer change is permitted.

2. **Record-candidate overlap RED/GREEN**
   - Extend the existing InnoDB scheduler MTR: a waiter changes its record-lock set after initial Phase1 and remains the survivor. RED must show `phase2_record_prebuilt_target_count=0` and nonzero Phase2 materialization.
   - Add only a coalesced stable-boundary hint ledger/API to the new scheduler files. Publish it both for an idle-active old transaction observed at T0 and for a later BODY exit that leaves the old transaction active. The hint contains connection incarnation, command sequence, and diagnostic thread id; it contains no HELD/support/permit/blocker or transfer state.
   - In the existing dependency readiness adapter in `sql/preserve_trx.cc`, consume one batch before each tick, re-pin/revalidate declared targets, call the existing Phase1 record scan API, sample the corresponding live fence, attach it through the exact additive candidate setter, enqueue through the already-open Phase1 batch sender, then flush once for the batch. Keep the existing active-progress caller and `(false,false,false)` tuple unchanged.
   - Linearize the last hint against normal HARD: command exit publishes the hint before storing `IDLE`, both under the scheduler mutex; if tick sees no executing BODY but the hint ledger is nonempty, it returns `RUNNING` for one owner refresh pass. Deadline HARD never waits for this optional work.
   - Candidate miss/staleness falls back to the existing final fence. Enqueue/flush failure returns the existing progress/source failure; it never becomes scheduler `SAFETY_ABORT`.

3. **Approved mode-independent source orchestration RED/GREEN**
   - First implement record candidate refresh only. Build Release and run a short identical dependency pressure diagnostic.
   - Retain r20 as the formal RED: exact `BODY→FINAL_ACK=2236116us`, `BODY→HARD=127194us`, `HARD→FINAL_ACK=2108922us`; record fallback is already only 31/863, while external staging waits dominate the CLOSING workers.
   - In the existing early pipeline for all modes, make workers capture candidates and enqueue final-HWM without synchronously staging external objects. The existing coordinator consumes completed candidates in waves: flush final-HWM, verify/publish each exact descriptor, stage that wave through the existing API, flush staged seeds, then publish prewarm LSN facts. Do not wait for all workers to join: a completed token must retain the existing early-seal behavior while other BODY commands still run. Do not modify transfer/receiver files or any runtime parameters.
   - First run the scheduler mode/support-ledger MTR and a dependency mixed smoke. A smoke miss is diagnostic only. Then run dependency mixed full and a legacy mixed comparison with the same release binary; only after both are correct run the full 300-second, 1000-thread, 128x20000 sysbench five-round gate.
   - Any functional failure, source lint violation outside the exact ordering span, dependency-only branch, or transfer/receiver diff requires reverting this optimization rather than widening cleanup or protocol logic.

4. **Cleanup and isolation**
   - Remove every temporary diagnostic before final verification.
   - Protected transfer/receiver hashes remain unchanged. Warmcopy differs only by the exact additive candidate setter, with all old methods source-normalized to baseline. `LEGACY`/OFF cannot call the hint consumer or setter.
   - Do not stage, commit, or push.

---

### Task 1: Freeze mode, capture, and ZERO_DIFF contracts

**Files:**
- Modify: `sql/preserve_trx.cc`
- Modify: `sql/preserve_trx.h`
- Modify: `sql/sys_vars.cc`
- Modify: `sql/sql_class.h`
- Modify: `mysql-test/suite/preserve_trx/my.cnf`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mode.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mode.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_mode.result`

- [ ] **Step 1: Record the protected hashes and clean-tree baseline**

Run:

```bash
git status --short -uall
shasum -a 256 sql/preserve_trx_transfer.cc sql/preserve_trx_transfer.h \
  sql/preserve_trx_lock_warmcopy.cc sql/preserve_trx_lock_warmcopy.h \
  storage/innobase/include/lock0warmcopy.h \
  storage/innobase/lock/lock0warmcopy.cc \
  sql/preserve_trx_drain.cc sql/preserve_trx_drain.h \
  unittest/gunit/preserve_trx-t.cc
```

Expected: hashes exactly match the manifest above; only the approved design/plan documents are untracked or modified.

- [ ] **Step 2: Capture the pre-edit baseline profile and binary identity**

Before the first production edit, build the current `9c6e6b1f...` Debug/Release binaries, record both SHA-256 values, and run the design's `baseline-legacy` MTR/E2E manifest plus the existing legacy mixed-transfer profile into a read-only evidence directory outside build outputs. Record the exact source HEAD, commands, startup options, test manifest, pass/fail/skip, and raw logs. This evidence is the baseline leg used in Task 13; do not overwrite it after rebuilding the worktree. The baseline binary does not know the new sysvar, so do not inject it.

- [ ] **Step 3: Write the RED mode/isolation MTR**

The test must assert:

```sql
SELECT @@GLOBAL.rds_preserve_trx_standby_phase2_scheduler_mode;
--error ER_WRONG_ARGUMENTS
SET GLOBAL rds_preserve_trx_standby_phase2_scheduler_mode=
  'LEGACY_READINESS_THEN_CLOSING';
```

Use a source restart with `STANDBY_TRANSFER_SAVE`, outbound host/port/user, and `DEPENDENCY_CONVERGENCE_V1`; configure mysqld.2 as the receiver. Add a second restart with `LEGACY_READINESS_THEN_CLOSING` and assert the existing drain result is unchanged.

- [ ] **Step 4: Run the test and retain RED evidence**

Run:

```bash
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --retry=0 \
  standby_transfer_phase2_scheduler_mode
```

Expected: FAIL because the new sysvar does not exist.

- [ ] **Step 5: Add the startup-only enum and compact POD**

Add these exact public concepts without including the scheduler header from `sql_class.h`:

```cpp
enum Preserve_trx_standby_phase2_scheduler_mode : ulong {
  PRESERVE_TRX_PHASE2_SCHEDULER_LEGACY = 0,
  PRESERVE_TRX_PHASE2_SCHEDULER_DEPENDENCY_CONVERGENCE_V1 = 1
};

extern ulong preserve_trx_standby_phase2_scheduler_mode;

enum class Preserve_trx_phase2_command_stage : uint8_t {
  IDLE = 0,
  ADMISSION_INFLIGHT,
  T0_CLAIMED_PRE_GATE,
  HELD,
  CUTOFF,
  PERMIT_RESERVED,
  NATIVE_PRE_BODY_EXIT,
  WAIT_NATIVE_RESTORE,
  EXECUTING
};
```

Add a per-THD POD containing a lazily assigned process-unique connection incarnation, an aggregate sequence protected by `LOCK_thd_data`, and an atomic stage. Do not reuse `preserve_trx_command_sequence` or the legacy risky/unknown depth as scheduler identity.

Define the sysvar names exactly as:

```text
LEGACY_READINESS_THEN_CLOSING
DEPENDENCY_CONVERGENCE_V1
```

Use product default `DEPENDENCY_CONVERGENCE_V1`, `READ_ONLY`, `NOT_IN_BINLOG`, and the same startup-only check style as artifact mode. Add `rds-preserve-trx-standby-phase2-scheduler-mode=LEGACY_READINESS_THEN_CLOSING` to the Preserve suite `my.cnf`.

- [ ] **Step 6: Define the cached source-capture predicate**

In `preserve_trx.cc/.h`, expose a fast predicate whose cached value is true only for Preserve ON + dependency mode + standby artifact + immutable outbound source-role configuration. Freeze it from `preserved_trx_leave_server_startup()` after startup options are parsed; use configured outbound host/port/user role facts, not live credential readiness or an active attempt. OFF/legacy/local/receiver must return before acquiring a new lock. Do not modify the existing `mysqld.cc` enter/leave call sites.

- [ ] **Step 7: Rebuild and run GREEN mode coverage**

Run:

```bash
cmake --build build-debug --target mysqld -j8
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --retry=0 \
  standby_transfer_phase2_scheduler_mode
```

Expected: build exit 0 and the test passes with product default dependency, suite default legacy, startup-only rejection, and legacy drain outcome intact.

---

### Task 2: Create the scheduler core and exact command linearization

**Files:**
- Create: `sql/preserve_trx_standby_phase2_scheduler.h`
- Create: `sql/preserve_trx_standby_phase2_scheduler.cc`
- Modify: `sql/CMakeLists.txt`
- Modify: `sql/preserve_trx.cc/.h`
- Modify: `sql/sql_parse.cc`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_protocol.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_protocol.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_protocol.result`

- [ ] **Step 1: Write the RED protocol/race MTR**

Use DBUG barriers to cover all orderings below:

```text
command reads route=null -> owner publishes route/T0 -> command BODY CAS
owner publishes route -> command publishes ADMISSION -> T0 enumerates
command EXECUTING CAS wins -> T0 registers T0_EXECUTING
T0 claim wins -> stale ALLOW cannot enter BODY
HARD cutoff wins -> command returns ER_PRESERVE_TRX_SESSION_DRAINED
```

The business assertion must update a row only in the EXECUTING-wins case. The cutoff cases must leave the row unchanged.

- [ ] **Step 2: Run and retain RED evidence**

Run:

```bash
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --retry=0 \
  standby_transfer_phase2_scheduler_protocol
```

Expected: FAIL because route/T0/stage DBUG points and scheduler behavior are absent.

- [ ] **Step 3: Define the narrow scheduler API**

The new header must expose only command/transaction facts and scheduler results. Use these stable concepts consistently throughout later tasks:

```cpp
class THD;

namespace preserve_trx_phase2_scheduler {

struct Command_key {
  uint64_t connection_incarnation;
  uint64_t sequence;
};

struct Transaction_key {
  uint64_t connection_incarnation;
  uint64_t ordinal;
};

enum class Gate_action : uint8_t {
  DEFER_TO_CLASS_GATE,
  ENTER_BODY,
  HELD,
  CUTOFF_4020,
  NATIVE_PRE_BODY_EXIT,
  RETRY_NATIVE
};

enum class Command_class : uint8_t {
  TX_PROGRESS,
  TX_END,
  TX_END_BY_DDL,
  DEFAULT_DENY
};

struct Admission_request {
  Command_key command;
  Transaction_key transaction;
  bool has_old_transaction;
  Command_class command_class;
  bool effective_no_chain;
};

struct Command_exit_fact {
  Command_key command;
  Transaction_key old_transaction;
  bool had_old_transaction;
  bool entered_body;
  bool old_transaction_ended;
};

enum class Terminal_result : uint8_t {
  RUNNING,
  HARD_QUIESCENT,
  HARD_DEADLINE,
  SAFETY_ABORT,
  OWNER_CANCELLED
};

struct Owner_config {
  uint64_t attempt_id;
  uint64_t generation;
  uint64_t owner_thread_id;
  uint64_t policy_started_us;
  uint64_t absolute_deadline_us;
};

class Attempt;
using Attempt_handle = std::shared_ptr<Attempt>;

Attempt_handle publish_and_register_t0(THD *owner,
                                       const Owner_config &config);
Gate_action gate_command(THD *command_thd,
                         const Admission_request &request);
void note_command_exit(const Command_exit_fact &fact);
void note_teardown_begin(uint64_t connection_incarnation);
void note_transaction_cleanup(uint64_t connection_incarnation,
                              const Transaction_key &transaction);
Terminal_result tick(const Attempt_handle &attempt, uint64_t now_us,
                     uint64_t tick_stop_us,
                     bool owner_progress_already_serviced);
bool cutoff_response_handoff_pending();
void wait_for_cutoff_response_handoff();
void owner_cancel(const Attempt_handle &attempt, uint32_t cause);
void publish_native_admission_restored_and_retire_route(
    const Attempt_handle &attempt);
void release_cutoff_responses_and_retire_route(
    const Attempt_handle &attempt);

}  // namespace preserve_trx_phase2_scheduler
```

Do not add any transfer, token, warmcopy, receiver, target, sender, ACK, or progress-callback type to this header.

- [ ] **Step 4: Implement two-level capture and route lifetime**

Implement:

```text
source capture: startup-cached bool + per-THD POD, active before T0
route active: process atomic fast flag + route mutex + shared Attempt
T0 lifetime: one opaque existing external-THD pin per registered connection,
             protected by scheduler-local probe_inflight borrows
callback lifetime: separate Attempt shared handle after route retirement
```

False route reads must not take the route mutex. Publishing a route precedes T0 enumeration. Expose a move-only opaque external-pin handle from `preserve_trx.h`, backed by the existing private pin registry; do not expose counters or move the registry into the scheduler. T0 acquires the handle under `LOCK_thd_data`; every tick must increment a per-entry `probe_inflight` borrow under the scheduler mutex before copying raw THD and decrement it after native locks are released and merge finishes. Terminal/teardown first disables new borrows. It may move the unique pin immediately only when `probe_inflight == 0`; otherwise it marks `pin_release_pending`, and the last probe exit moves it exactly once. Every actual pin release occurs outside the scheduler mutex. Route/callback lifetime thereafter contains identities only. Normal route retirement may run only after existing CLOSING publication, authoritative target collection, and exact target lifetime-pin handoff; abort retirement runs only after native-admission restore publication.

- [ ] **Step 5: Implement the stage protocol**

Use one CAS state word per connection:

```text
IDLE
  -> ADMISSION_INFLIGHT
       -> T0_CLAIMED_PRE_GATE
       -> HELD -> PERMIT_RESERVED
       -> CUTOFF
       -> NATIVE_PRE_BODY_EXIT
       -> WAIT_NATIVE_RESTORE
  T0_CLAIMED_PRE_GATE
       -> HELD | PERMIT_RESERVED | CUTOFF |
          NATIVE_PRE_BODY_EXIT | WAIT_NATIVE_RESTORE
  PERMIT_RESERVED -> EXECUTING
  PERMIT_RESERVED -> HELD on support loss for support-dependent permits
  ADMISSION_INFLIGHT / T0_CLAIMED_PRE_GATE / HELD / PERMIT_RESERVED
       -> CUTOFF on normal/deadline HARD
  ADMISSION_INFLIGHT / T0_CLAIMED_PRE_GATE / HELD / PERMIT_RESERVED
       -> WAIT_NATIVE_RESTORE on owner/safety abort
  EXECUTING / CUTOFF / NATIVE_PRE_BODY_EXIT -> IDLE on exact retirement
  WAIT_NATIVE_RESTORE -> IDLE only after native admission is restored
```

The POD publishes sequence/class facts before release-publishing `ADMISSION_INFLIGHT`. T0 claim is the CAS to `T0_CLAIMED_PRE_GATE`; `T0_PRE_GATE/T0_EXECUTING` are scheduler-ledger classifications, not additional POD states. T0 claim, BODY commit, and HARD cutoff compete on the same stage. Sequence does not change until the stage returns to IDLE. Every callback carries connection incarnation + sequence; `thread_id` alone is never an identity.

`gate_command()` may wait only on the calling business command thread. For HELD it releases the scheduler mutex, sleeps for at most 5ms, then reacquires the active attempt and revalidates revision, stage, terminal state, support and `command_thd->killed`. Do not put every HELD connection on a repeated timed condition wait: full pressure turns that into millions of platform timed-wait calls without improving the scheduler's 5ms proof cadence. Owner change waiting and the one-shot response latch may continue to use the scheduler condition. On native interruption, CAS to `NATIVE_PRE_BODY_EXIT`, return without injecting 4020, and let `dispatch_command()` preserve the native kill/connection error. Do not retain `command_thd` in the ledger.

- [ ] **Step 6: Register the new source and add thin facades**

Add one source entry to `sql/CMakeLists.txt`. `sql_parse.cc` calls only facade functions declared in `preserve_trx.h`; it does not include the scheduler header. Only `preserve_trx.cc` and the scheduler implementation may directly consume the scheduler API.

- [ ] **Step 7: Run focused GREEN verification**

Run:

```bash
cmake --build build-debug --target mysqld -j8
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx --retry=0 \
  standby_transfer_phase2_scheduler_protocol
```

Expected: build exit 0; every race ordering reaches its deterministic row/error result.

---

### Task 3: Integrate text, PS, generic, and no-response command gates

**Files:**
- Modify: `sql/preserve_trx.cc/.h`
- Modify: `sql/sql_parse.cc`
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_protocol.test`

- [ ] **Step 1: Extend the RED protocol test with exact command classes**

Cover:

```text
COM_QUERY: BODY CAS in mysql_execute_command final gate
COM_STMT_EXECUTE: precheck success -> stmt->lex classification -> BODY CAS
unknown PS id / wrong parameter count: native error, NATIVE_PRE_BODY_EXIT
COM_FIELD_LIST / COM_REFRESH: T0-before-BODY generic CAS, then new-arrival default deny
COM_STMT_PREPARE / COM_STMT_FETCH: default deny HELD then 4020
COM_STMT_CLOSE: native close in SOFT, private HARD, and abort window
COM_STMT_SEND_LONG_DATA: native buffer append until existing CLOSING
COM_CLONE: successful deferred BODY exits after execute_server;
           load/pre-gate/cutoff failures exit at common done
multi-statement / CALL / XA: default deny
drain owner / system or internal THD / HA control / RESUME / SHUTDOWN: bypass
non-classic Srv_session and replication init_slave: internal bypass
ordinary classic init_connect: synthetic COM_QUERY admission before direct dispatch
COM_PING: exact native liveness bypass without BODY
COM_STATISTICS / COM_PROCESS_INFO / COM_CHANGE_USER / COM_RESET_CONNECTION:
  ordinary default deny; no generic status/control bypass
HELD command: support wake, HARD wake, abort/restore, and KILL CONNECTION
```

For LONG_DATA, prepare a statement with a multi-chunk parameter, force scheduler abort/native restore, execute it, and assert the complete parameter value was written.

- [ ] **Step 2: Run and retain RED evidence**

Run the focused protocol MTR. Expected: new cases fail before gate integration.

- [ ] **Step 3: Implement the three BODY commit points**

Use exactly:

```text
COM_QUERY
  -> generic protocol gate keeps ADMISSION
  -> parsed final SQL gate classifies LEX and commits BODY

COM_STMT_EXECUTE
  -> generic protocol gate keeps ADMISSION
  -> mysql_stmt_precheck runs unchanged
  -> successful precheck uses stmt->lex and commits BODY
  -> mysqld_stmt_execute runs unchanged

other ordinary response-producing protocol commands
  -> existing protocol gate commits BODY or blocks
```

Do not modify `sql/sql_prepare.cc/.h`. Prepared inner `mysql_execute_command()` only validates the already-EXECUTING sequence and never obtains another permit. Apply actor filtering before ordinary admission in this order: feature/mode/source capture, system/internal/non-classic, HA control, drain owner, exact `COM_PING` plus no-response cleanup exceptions, ordinary business command. Do not create a generic status/control bypass. `execute_init_command()` is not itself an internal-actor proof: wrap ordinary classic `init_connect` in a synthetic aggregate COM_QUERY sequence before its direct dispatch, while replication `init_slave` remains bypassed by the system-THD predicate.

- [ ] **Step 4: Implement exact aggregate exit**

SQL/PS RAII clear helpers notify the scheduler only after the existing aggregate depth reaches zero and after releasing `LOCK_thd_data`. Add an idempotent generic exit at common `done:` after the switch BODY has returned but before `send_statement_status()`. Only when `clone_cmd != nullptr` and execution has actually been delegated to the post-response clone BODY may that common exit defer retirement; retire that path after `execute_server()` returns. A `COM_CLONE` allocation/load failure, an error before its switch case, or a scheduler cutoff still retires at common `done:`. Parse/precheck failures retire as `NATIVE_PRE_BODY_EXIT` without changing the native diagnostics area.

- [ ] **Step 5: Preserve no-response behavior**

Do not introduce a scheduler DROP action. `COM_QUIT`, `COM_STMT_CLOSE`, and `COM_STMT_SEND_LONG_DATA` do not consume permits. Only the already-existing CLOSING gate may silently drop LONG_DATA after future EXECUTE is irreversibly cutoff.

- [ ] **Step 6: Run GREEN protocol verification**

Run the focused MTR and assert every error number, final table value, and prepared-statement lifecycle result, including deterministic clone load failure, pre-gate native error, scheduler cutoff, and successful deferred clone retirement.

---

### Task 4: Add exact transaction lineage and allowlist admission

**Files:**
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Modify: `sql/preserve_trx.cc/.h`
- Modify: `storage/innobase/include/trx0preserve.h`
- Modify: `storage/innobase/trx/trx0preserve.cc`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_lineage.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_lineage.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_lineage.result`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_ddl.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_ddl.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_ddl.result`

- [ ] **Step 1: Write RED lineage and DDL cases**

First cover the exact classifier, default deny, a non-cohort business command returning 4020, a post-terminal command returning 4020 before HARD, COMMIT/ROLLBACK effective CHAIN/RELEASE, old transaction ending, failed COMMIT with old transaction still active, and each DDL class without support remaining HELD/cutoff. Add one `SET autocommit=0; UPDATE ...` connection that is idle at command boundary while its T0-active transaction holds a row lock: T0 must reserve `PENDING_T0_ACTIVE_IDENTITY`, and its next owner-thread COMMIT/ROLLBACK gate seals that same ordinal without creating a post-T0 transaction. Add a second `autocommit=0; UPDATE ...` connection whose next ordinary command is HELD through HARD: before the batch-predicate alignment this must deterministically fail the drain with 4013; afterward it must publish `QUIESCED` and appear as an additional survivor. Separately add a deterministic `autocommit=0` case whose UPDATE has crossed BODY at T0 but is paused before InnoDB starts its transaction. Positive support-dependent DML/DDL and DDL pre-commit outcome cases are added to the same files in Task 7, after native proof and ledger integration exist.

- [ ] **Step 2: Run and retain RED evidence**

Run both tests with `--retry=0`. Expected: fail before transaction ordinal and allowlist implementation.

- [ ] **Step 3: Implement immutable ordinal identity**

For every transaction already active at T0, reserve an ordinal from a SQL-owned transaction-presence fact copied under `LOCK_thd_data`: active multi-statement state, `OPTION_BEGIN`/`OPTION_NOT_AUTOCOMMIT`, and existing session-scope InnoDB participant/raw cookie. Do not call only the narrower `preserve_trx_has_explicit_active_transaction()` and do not inspect engine state/version here. Store `PENDING_T0_ACTIVE_IDENTITY` plus the opaque raw cookie; this includes idle `autocommit=0` holders as well as executing connections. A lock_sys exact holder/waiter snapshot or that connection's owner-thread stable gate/exit may seal raw cookie + version onto this existing ordinal. Pending identity cannot receive a permit; no-transaction closes it, and conflicting cookie/version safety-aborts. A closed ordinal never reopens. Exact identity comparison includes connection incarnation, ordinal, and engine identity/version facts; it never relies on `thread_id` or raw `trx_t *` alone.

Expose this exact SQL-active predicate from the new scheduler module. In `preserve_trx.cc`, use `explicit-active || dependency-active` only in authoritative batch target admission, pending-boundary publication, and QUIESCED/ATTACHING validation. Do not use it to reinterpret session-only targets, phase1 prebuild eligibility, transfer tokens, or receiver state. This is the minimal fix for the demonstrated `autocommit=0 + EXACT_ACTIVE + participant -> DRAINED_NO_TRANSACTION -> 4013` mismatch; Legacy/OFF remain byte-for-byte on the explicit-active branch.

There is one narrow and distinct late-adoption rule: if an exact `T0_EXECUTING` sequence had no active transaction at T0, mark its lineage slot `PENDING_T0_BODY_FIRST_TX`. An exact engine sample or command-exit callback for that same sequence may atomically create the first old ordinal for the transaction produced by that BODY. No post-T0 command may create an ordinal. If the sequence exits with no active transaction, close the pending slot as no-transaction; if more than one engine identity is observed, safety-abort as lineage drift. Sealing a T0-reserved `PENDING_T0_ACTIVE_IDENTITY` ordinal is not late-adoption. This is scheduler lineage handling, not the separate general `autocommit=0` Preserve-admission change.

Add two narrow non-allocating InnoDB-session APIs in `trx0preserve.h/.cc`: an external collector peek that, under `THD::LOCK_thd_data`, returns only the existing raw engine cookie without dereferencing `trx_t::state/version`; and an owner-thread identity snapshot for stable BODY gate/exit that returns raw cookie + `trx_t::version` or a retry/unknown status on lifecycle transition. Transaction presence itself comes from the SQL-owned facts above, not from an external engine-state read. Never call allocating `thd_to_trx()` from T0/scanner. Every externally observed T0-active transaction remains `PENDING_T0_ACTIVE_IDENTITY` until Task 5's lock_sys exact probe or its own command thread confirms it.

- [ ] **Step 4: Implement explicit allowlists**

Implement explicit switches for:

```text
TX_PROGRESS:
  SELECT, INSERT, INSERT_SELECT, UPDATE, MULTI_UPDATE,
  DELETE, MULTI_DELETE, REPLACE, REPLACE_SELECT,
  SAVEPOINT, RELEASE_SAVEPOINT

TX_END:
  COMMIT, ROLLBACK with effective_tx_chain=false

TX_END_BY_DDL:
  ALTER_TABLE, CREATE_INDEX, DROP_INDEX, RENAME_TABLE, TRUNCATE
```

Do not derive admission from `CF_CHANGES_DATA`.

Compute terminal semantics from parsed `LEX` plus `@@completion_type`, not SQL text:

```text
effective_tx_chain =
    lex.tx_chain == YES
    || (@@completion_type == 1 && lex.tx_chain != NO)

effective_tx_release =
    lex.tx_release == YES
    || (@@completion_type == 2 && lex.tx_release != NO)
```

Only `effective_tx_chain=false` may receive a `TX_END` terminal permit. Preserve native `effective_tx_release` disconnect behavior.

- [ ] **Step 5: Implement terminal command exit verification**

Permit marks old lineage `TERMINALIZING`; it does not close it. On exit, inspect the exact old transaction identity:

```text
old transaction ended -> close ordinal and remove incoming/outgoing support
old transaction active -> preserve native error and SAFETY_ABORT
DDL implicit pre-commit ended old transaction -> close old ordinal even if DDL BODY failed
```

- [ ] **Step 6: Run the lineage/classification GREEN slice**

Run both MTRs. Expected at this task boundary: exact classification, default-deny, idle-active T0 ordinal reservation plus owner-thread sealing, TX_END/CHAIN, terminal failure, no-support DDL cutoff, and T0-BODY late adoption pass without changing native errors. Keep lock_sys sealing and positive support-dependent DML/DDL assertions for Tasks 5/7; do not claim their GREEN result here.

---

### Task 5: Implement bounded InnoDB exact blocker proof

**Files:**
- Modify: `storage/innobase/include/lock0preserve_plan.h`
- Modify: `storage/innobase/lock/lock0preserve.cc`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_innodb.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_innodb.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_innodb.result`

- [ ] **Step 1: Write the RED InnoDB matrix**

Cover a waiter blocked by two granted S holders, two waiters sharing one blocker, waiter disconnect, granted + waiting predecessor, lock_sys busy retry, >256 predecessor incomplete result, RR positive record/table proof, RC/RU record S denial, RC/RU record X denial, and AUTO_INC denial. Include a command already in lock wait before T0. Make one granted holder idle with an already-active T0 transaction and assert the lock_sys snapshot seals its pre-existing `PENDING_T0_ACTIVE_IDENTITY` ordinal; also inject identity lifecycle drift and require retry/UNKNOWN without partial edges.

- [ ] **Step 2: Run and retain RED evidence**

Run the InnoDB MTR. Expected: fail because exact queue proof and support are absent.

- [ ] **Step 3: Add fixed-capacity DTOs**

Use caller-owned storage and statuses equivalent to:

```cpp
struct trx_preserve_phase2_identity {
  uint64_t immutable_id;
  uint64_t version;       // trx_t::version, never trx_locks_version
  uintptr_t thd_cookie;
};

enum class lock_preserve_phase2_probe_status : uint8_t {
  NOT_WAITING,
  RETRYABLE_LOCK_SYS_BUSY,
  COMPLETE,
  UNSUPPORTED_PENDING_PREDECESSOR,
  UNSUPPORTED_RELEASE_CLASS,
  UNKNOWN_INCOMPLETE,
  UNKNOWN_IDENTITY
};
```

The API accepts a fixed buffer and capacity 256. It performs no allocation while holding lock_sys. The exact identity is pinned T0 connection incarnation + raw trx cookie/`trx_immutable_id()` + existing `trx_t::version`; the version increments at transaction start. Never substitute `trx->lock.trx_locks_version`, which changes as locks are added/removed. `trx_t::version` is copied only while the exact probe owns the native queue snapshot and has revalidated the same waiter/holder; SQL-layer `LOCK_thd_data` is not its protection. The DTO is compared after the native snapshot but never dereferenced outside the protecting pin/latch protocol.

- [ ] **Step 4: Reuse and verify the non-allocating THD-to-trx peek**

Use Task 4's external peek while holding a scheduler `probe_inflight` borrow, the T0 external THD pin, and `THD::LOCK_thd_data` to read existing InnoDB handlerton data with `thd_get_ha_data()`. Copy only an opaque raw trx cookie; do not read `trx_t::version` there. Never call `thd_to_trx()` from the scheduler because it may allocate an InnoDB session. Release `LOCK_thd_data` before acquiring lock_sys. The raw cookie exists only in the local probe input/immutable identity value; the ledger never retains a dereferenceable `trx_t *`.

- [ ] **Step 5: Implement exact queue traversal**

Use the 8.0.22 `Global_exclusive_try_latch`, verify the waiter identity/`trx_t::version`/state, and enumerate the exact queue prefix before `wait_lock` with the primitives already available to `lock0preserve.cc` through `lock0priv.h`: record locks use `lock_rec_get_first_on_page_addr()` plus `lock_rec_get_next_on_page_const()` until `wait_lock`; table locks use `UT_LIST_GET_FIRST(table->locks)` plus `UT_LIST_GET_NEXT(tab_lock.locks, ...)` until `wait_lock`. Apply the existing `lock_has_to_wait()` predicate to each predecessor. A complete snapshot may seal a matching T0-reserved waiter or granted-holder `PENDING_T0_ACTIVE_IDENTITY` ordinal before publishing edges; it must never create an ordinal for a transaction absent at T0. Do not invent or import a newer-version `lock_queue_iterator_get_prev()` API. Count every predecessor step, including compatible entries, against 256. Mapping is request-level all-or-none: any incompatible waiting predecessor, unmappable/non-T0 granted holder, unsupported release class, truncation, or identity uncertainty prevents publication of every holder subset. A busy latch is retryable; incomplete identity/snapshot is fail-closed UNKNOWN.

- [ ] **Step 6: Apply release-class policy**

Use an exact v1 release-class switch: allow RR/SERIALIZABLE ordinary record S/X and, independently of isolation, non-AUTO_INC table IS/IX/S/X; reject RC/RU ordinary record S/X, AUTO_INC, unknown mode/flags, PREDICATE, and PRDT_PAGE as `UNSUPPORTED_RELEASE_CLASS`. Do not infer support from “not AUTO_INC”, add per-lock fields, or modify acquire/release hot paths.

- [ ] **Step 7: Run the exact-probe GREEN slice**

Run the focused MTR using diagnostics/DBUG oracles for DTO/status output. Expected at this task boundary: complete all-blocker snapshots, negative/unsupported/retryable/unknown statuses, cap accounting, and no partial output are correct. Final support/permit business behavior remains RED until Task 7.

---

### Task 6: Implement bounded MDL exact blocker proof

**Files:**
- Modify: `sql/mdl.h`
- Modify: `sql/mdl.cc`
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mdl.test`
- Create: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mdl.cnf`
- Create: `mysql-test/suite/preserve_trx/r/standby_transfer_phase2_scheduler_mdl.result`

- [ ] **Step 1: Write the RED MDL matrix**

Cover T0-before-wait and T0-after-wait, two granted holders, an incompatible waiting predecessor, statement-duration denial, explicit-duration denial, transaction-duration positive proof, try-read busy retry, disconnect, and stale ticket replacement.

- [ ] **Step 2: Run and retain RED evidence**

Run the MDL MTR. Expected: fail because release builds cannot yet identify pending wait duration.

- [ ] **Step 3: Add the wait-duration marker**

Add one `enum_mdl_duration` field beside `MDL_context::m_waiting_for`, initialized to `MDL_DURATION_END`. Extend `will_wait_for()` with an optional duration argument. In the existing `m_LOCK_waiting_for` write critical section, publish and clear ticket + duration together. Non-MDL waits pass `MDL_DURATION_END`. Gate the extra marker write with the startup-cached source-capture predicate, not the active route.

- [ ] **Step 4: Add a local instrumented PR try-read guard**

Implement only in `mdl.cc`:

```text
PSI start with PSI_RWLOCK_TRYREADLOCK
native_mutex_trylock(prlock.m_prlock.lock)
busy -> PSI end with busy and return false
success -> increment active_readers, unlock native mutex
release -> existing instrumented PR unlock path
```

Do not modify `mysys`, component services, or public rwlock APIs.

- [ ] **Step 5: Implement immutable demand and owner proof**

Acquire try-read locks in this order:

```text
m_LOCK_waiting_for
  -> waiting ticket MDL_lock::m_rwlock
  -> revalidate ticket, duration, and WS_EMPTY
  -> copy key/type and complete conflict facts
  -> release in reverse order
```

Use native waiting-priority conflict rules, including `is_incompatible_when_waiting()`. Any priority-conflicting request found in the complete waiting queue rejects the whole request. Publish only a fingerprinted immutable demand with `demand_generation` and expiry. At blocker final gate, and only on that blocker's own command thread, match transaction-duration tickets against a pre-maintained `(MDL_key, waiter)` demand index; if positive matches exist, scan explicit-duration tickets with the same total budget and remove matches whose conflict survives COMMIT. Never scan another THD's context-private ticket store. Cap the combined ticket visits, same-key candidate examinations, and positive matches at 256; return `UNKNOWN_INCOMPLETE` on truncation, and revalidate command/ordinal/demand generation before merge. A new/changed positive demand increments decision revision and wakes HELD gates; same-fingerprint refresh does not repeatedly broadcast.

- [ ] **Step 6: Run the MDL proof GREEN slice**

Run the focused MTR using demand/status diagnostics. Expected at this task boundary: transaction-duration demand, statement/explicit/pending-predecessor rejection, try-read retry, generation replacement, cap handling, and stale revalidation are correct. Final MDL support/permit behavior remains RED until Task 7.

---

### Task 7: Complete support ledger, bounded rounds, and permit revocation

**Files:**
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_innodb.test`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mdl.test`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_lineage.test`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_ddl.test`

- [ ] **Step 1: Add RED support-lifecycle cases**

Cover 5ms refresh, 10ms expiry, two waiter votes for one blocker, one waiter disconnect, all blockers for one waiter, support expiry while command is HELD, TX_PROGRESS and TX_END_BY_DDL expiry/disconnect after `PERMIT_RESERVED`, no-support TX_END reservation, support expiry after EXECUTING, stale snapshots, partial rounds, complete rounds, UNKNOWN fail-closed, and the positive DML/DDL/pre-commit cases deferred from Task 4.

- [ ] **Step 2: Implement the bidirectional edge ledger**

Each edge key is exact waiter command + blocker transaction + lock domain; an MDL edge additionally carries the matched demand fingerprint/generation and is invalid when that generation changes. Maintain both waiter-outgoing and blocker-incoming indexes. Upsert/refresh one edge atomically. Removing a waiter deletes only its outgoing edges; ending a blocker deletes only incoming edges to that ordinal. Fresh-support count is derived under the scheduler mutex, never maintained by an unverified external counter.

- [ ] **Step 3: Implement one bounded tick**

Use constants:

```cpp
constexpr uint64_t kScanPeriodUs = 5000;
constexpr uint64_t kTickBudgetUs = 2000;
constexpr uint64_t kProbeStartReserveUs = 500;
constexpr size_t kMaxQueuePredecessors = 256;
constexpr uint64_t kSupportLifetimeUs = 10000;
```

The tick never sleeps or invokes progress. It receives `now_us`, `tick_stop_us`, and whether owner progress was already serviced. With no `T0_EXECUTING/EXECUTING` candidate it performs no native scan. At round start, copy a stable sorted vector of exact Command_keys into reusable storage; cursor only walks that vector, new BODY commands enter the next round, and exited keys stale-discard. Prioritize re-probing waiters with fresh support, but never extend an edge from liveness alone: InnoDB requires a new COMPLETE snapshot, and MDL requires a fresh owner-local match against the current demand generation. Advance the cursor and save round state on budget exhaustion.

Every candidate follows this non-nesting lock order:

```text
scheduler mutex: validate active entry, probe_inflight++, copy command/raw THD
  -> unlock scheduler mutex
borrow now prevents move/release of the T0-held external THD lifetime pin
  -> THD::LOCK_thd_data
  -> copy command POD, existing ha_data, and opaque raw engine cookie only
  -> unlock THD::LOCK_thd_data
InnoDB try-X or MDL try-read: build immutable fixed-capacity DTO
  -> release every native lock
scheduler mutex: revalidate generation/sequence, merge or stale-discard,
                 probe_inflight--, move pending pin if this is the last borrower
  -> unlock scheduler mutex and release any moved pin
```

Keep the T0 lifetime pin until terminal/teardown retires that connection, subject to the borrow rule above; tick never reacquires it through the global registry. T0 acquisition alone uses the baseline collector order `LOCK_thd_data -> existing pin mutex`. Never hold the scheduler mutex together with a THD, external-pin-global, InnoDB, or MDL lock; never hold a native lock with either scheduler or THD lock. Add a scope guard so every retry, stale, unsupported, unknown, or early-return probe decrements the borrow exactly once.

- [ ] **Step 4: Implement round completeness**

For InnoDB, only a complete request snapshot replaces that waiter's InnoDB outgoing set. For MDL, scan publishes/replaces a fingerprinted demand generation; owner gates create edges bound to that current generation. A changed/NOT_WAITING/UNSUPPORTED MDL demand retires old-generation edges. `COMPLETE` merges all-or-none; complete `NOT_WAITING/UNSUPPORTED_*` replaces with empty; `RETRYABLE_BUSY/STALE` and stale-discard retain prior facts only until natural expiry; `UNKNOWN_*` merges nothing and safety-aborts. Only a fully consumed stable round may remove an old domain fact because its command key was absent; partial rounds never use absence.

- [ ] **Step 5: Implement permit ordering**

Under the scheduler mutex, perform expiry-first, ordinal validation, HARD/abort validation, support validation, and `HELD -> PERMIT_RESERVED`. BODY CAS is the only transition to EXECUTING. Support loss revokes reserved support-dependent `TX_PROGRESS` and `TX_END_BY_DDL`, but not no-support `TX_END`; HARD/abort revokes every reserved class. Any EXECUTING command is never rolled back to a scheduler state. A new/changed positive MDL demand broadcasts once so an already HELD potential blocker can run its bounded owner-local match and reserve in its own thread.

- [ ] **Step 6: Run the focused GREEN matrix**

Run the InnoDB, MDL, lineage, and DDL MTRs together. Expected: support counts and final business data match every lifecycle case, including DDL reserved-permit revocation and post-BODY terminal facts.

---

### Task 8: Embed the owner policy and HARD handoff without touching the pipeline

**Files:**
- Modify: `sql/preserve_trx.cc/.h`
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_mode.test`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_protocol.test`

- [ ] **Step 1: Write RED owner/HARD/abort cases**

Cover pre-T0 deadline/owner-killed first winner, empty T0, normal HARD, deadline HARD with BODY still active, active-progress failure, proof UNKNOWN, invariant failure, the private HARD-to-CLOSING overlap window, initial progress timestamp 0, a progress deadline within 2ms, and a deliberately overlong progress callback. Assert a pre-T0 terminal winner creates no scheduler route/attempt, and empty T0 performs no extra progress call. Do not add RESET DRAIN race scenarios or a RESET scheduler MTR matrix; the only RESET contract is the static unsupported boundary before existing ownership `request_reset()`.

- [ ] **Step 2: Implement the dependency owner adapter at the existing seam**

At the current `preserve_trx_wait_for_phase1_readiness()` call site, branch first on the frozen attempt mode. Passive final-record timestamp capture may sit next to the branch, but it must not add a semantic precheck or route to legacy:

```text
LEGACY -> call the original helper unchanged
DEPENDENCY -> capture policy_started_us
  -> existing-equivalent reset / owner-killed / absolute-deadline first winner
  -> publish route and register T0 only if no terminal won
  -> existing owner loop:
  reset / owner killed / absolute deadline
  original active progress if due
  at most one scheduler tick if due
  terminal check
  bounded wait until nearest deadline
```

The dependency-only ordering is therefore `policy start -> existing-equivalent first-winner precheck -> publish route/T0 -> owner loop`; never construct a scheduler attempt merely to report an already-expired deadline. The legacy ordering remains the original helper call, including its internal `DEBUG_SYNC` and reset/killed/deadline checks.

Do not pass the active-progress callback into the scheduler. Preserve the existing closure, 50ms timestamp update, parameters `(false,false,false)`, and error exit.

When the initial progress timestamp is zero and T0 has executing commands, service the original progress once before the first due tick in the same owner iteration. If an unserviced progress deadline is within the tick budget, preserve the candidate cursor and return to owner without starting a native probe. An overlong callback does not cause a second progress call in the same iteration and does not starve the next scheduler tick.

- [ ] **Step 3: Implement terminal mapping**

```text
HARD_QUIESCENT / HARD_DEADLINE
  -> disable new probe borrow
  -> QUIESCENT proves inflight=0 and moves pins immediately;
     DEADLINE defers any busy pin until last probe exit
  -> finish all pending borrow release before existing CLOSING publication
  -> existing reset recheck
  -> existing WARMCOPY_CLOSING transition
  -> existing closing_command_gate_published release store
  -> existing target collection
  -> keep the existing target-classification mutex held
  -> move the existing exact target-pin collection into this barrier
  -> require one lifetime pin for every authoritative transaction target
  -> release the target-classification mutex
  -> keep frozen 4020 responses quarantined and keep scheduler route active
  -> run the existing CLOSING pipeline unchanged through the existing
     abort_phase1_transfer_targets_not_quiesced() call
  -> only after that call succeeds, release frozen 4020 responses under the
     attempt mutex
  -> retire scheduler route separately under the route mutex
  -> move the collected pins into the unchanged existing worker scope

OWNER_CANCELLED / active-progress failure
  -> freeze WAIT_NATIVE_RESTORE
  -> disable new probe borrow; move zero-inflight pins and defer busy pins
  -> existing abort_batch_transfer_epoch / abort_drain_participants
  -> publish native_admission_restored, then retire route

SAFETY_ABORT
  -> same pre-CLOSING cleanup class
  -> disable new probe borrow; move zero-inflight pins and defer busy pins
  -> existing unsupported error
  -> publish native_admission_restored, then retire route
  -> no CLOSING and no source-authority restore API
```

Normal HARD waits only for proof that pre-BODY commands are atomically CUTOFF; it does not wait for clients to receive or acknowledge 4020. CUTOFF 命令先退休 scheduler record 与 inflight marker，在公共 `done:` 标记 4020 deferred，但继续完成原生 audit/P_S/query/memroot/command cleanup 并从本次 `dispatch_command()` 返回。连接线程进入下一次 `do_command()`、尚未清空 diagnostics 或读取新 packet 的真实 idle boundary 时，才发布 existing idle/pending-quiesce、等待 response latch，并在 latch 打开后发送上一条 4020。The same owner opens the latch immediately after the existing non-survivor prune succeeds, including the existing no-quiesced-target branch. A failure before normal release follows the existing owner cleanup and native-admission-restore path, which must also open the latch.

Do not park a command inside the scheduler/native gate, and do not publish `m_server_idle` while its old `dispatch_command()` stack is still alive. The latter lets the preserve worker attach and detach the THD concurrently with native command cleanup. HARD-frozen commands and packets that first encounter the native CLOSING gate both return CUTOFF, complete the non-BODY command exit, clear their inflight marker, and defer only `send_statement_status()` plus its after-response notification. After the full dispatch cleanup returns, a thin hook at the next command-read entry publishes the real idle/pending-quiesce boundary and waits on the scheduler response latch. Keep the existing target-classification mutex solely for accepted-before-CLOSING packet classification and exact target-pin acquisition; keep COM_QUIT/COM_STMT_CLOSE and other approved no-response cleanup on their native path. Both old HELD commands and newly arriving CLOSING commands therefore become externally visible only after the same prune-complete signal. This wait is dependency-route gated; legacy does not enter it.

The scheduler receives no target ID, token ID, prune count, transfer status, or callback. The owner emits only a parameter-free “response may now be released” signal after the existing prune call returns success. Do not move, parallelize, skip, batch, or otherwise modify the prune call, and do not change transfer/receiver APIs or their error mapping.

- [ ] **Step 4: Keep RESET out of the scheduler state machine**

When a dependency attempt is active, the RESET control command reaches the existing handler and returns `ER_PRESERVE_TRX_UNSUPPORTED` before `request_reset()`. When no dependency attempt is active, existing behavior remains. Legacy behavior remains unchanged.

- [ ] **Step 5: Write the deterministic prune-before-response RED**

Extend the existing protocol MTR with the existing
`preserve_trx_batch_after_targets_quiesced_before_attach` debug-sync barrier.
While the owner is stopped immediately before the non-survivor prune, assert
that both a HARD-frozen response and a post-CLOSING response-producing command
are still blocked, and that the latter is already `Sleep` at the safe
post-dispatch idle boundary. The unsafe pre-response implementation must fail
the idle-boundary assertion. Then release the owner, require the original prune
to complete, and require both commands to receive native 4020. Do not add a
GUnit or RESET scenario.

- [ ] **Step 6: Run focused GREEN owner tests**

Run the scheduler mode and protocol groups. Expected: all terminal mappings,
HARD/CLOSING handoff orderings, and prune-before-response ordering pass. Do not
add or run a dependency RESET scheduler test; RESET remains only the static
unsupported boundary above.

- [ ] **Step 7: Verify protected caller text immediately**

Run:

```bash
rg -n -C 12 'stream_active_transfer_binlog_cache_progress' sql/preserve_trx.cc
```

Expected: the two function-anchored call sites show active progress `(false,false,false)` and pending progress `(false,true,final_hwm_async_capable)`; scheduler facts do not enter either argument/target set. Task 11's normalized source guard is authoritative after line numbers shift.

---

### Task 9: Compose teardown with the existing external-pin barrier

**Files:**
- Modify: `sql/preserve_trx.cc/.h`
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Test: `mysql-test/suite/preserve_trx/t/standby_transfer_phase2_scheduler_lineage.test`

- [ ] **Step 1: Add RED disconnect/late-callback cases**

Cover waiter disconnect, blocker disconnect with one and two supporting waiters, disconnect while BODY executes, disconnect after private HARD, thread-id reuse, late exact-probe result, and the pin-only `preserved_trx_wait_for_external_thd_use()` path. Add deterministic barriers for tick-vs-teardown and abort-vs-probe: pause after `probe_inflight++`/raw THD copy, publish teardown or abort, then prove the pin is not released until the probe exits and is released exactly once afterward.

- [ ] **Step 2: Refactor the existing pin-only barrier**

Extract the current pin-count wait into a private helper that does not publish scheduler RETIRING or transaction cleanup. Make `preserved_trx_wait_for_external_thd_use()` call only that helper.

- [ ] **Step 3: Implement tombstone-first teardown begin**

Inside existing `preserved_trx_begin_external_thd_teardown()`:

```text
global pin mutex: insert teardown tombstone
unlock global pin mutex
scheduler mutex: mark RETIRING, retire command/support,
                 move pin iff probe_inflight==0, else pin_release_pending=true
unlock scheduler mutex
release any moved pin
global pin mutex: wait existing pin_count == 0
```

The scheduler step first disables new borrows. It moves the pin only when `probe_inflight == 0`; otherwise it sets `pin_release_pending`, and the last probe exit moves/releases it outside the scheduler mutex. Never hold scheduler and global pin mutexes together, and do not add a second wait: the existing global pin-count barrier naturally waits for this pending pin.

- [ ] **Step 4: Implement transaction-cleanup end**

Inside existing `preserved_trx_end_external_thd_teardown()`, while the tombstone still exists, publish the exact transaction-cleanup fact through the callback-lifetime handle, close the ordinal, and remove incoming support. Then erase the tombstone and notify existing waiters. `sql/sql_class.cc` remains byte-for-byte unchanged.

- [ ] **Step 5: Run GREEN teardown verification**

Run lineage and InnoDB/MDL disconnect cases. Expected: no hang, no stale support, no deferred statement cleanup, and no new pin after tombstone publication.

---

### Task 10: Add exact terminal diagnostics without creating a downstream control path

**Files:**
- Modify: `sql/preserve_trx_standby_phase2_scheduler.h/.cc`
- Modify: `sql/preserve_trx.cc/.h`
- Test: all seven scheduler MTR groups

- [ ] **Step 1: Add RED final-record oracle cases**

Cover one record per attempt, early failure without epoch, duplicate attempt id rejection, generation mismatch, empty eligible set, exact eligible set, deadline coverage incomplete, late exit after frozen snapshot, and last BODY exit later than FINAL ACK rejection.

- [ ] **Step 2: Freeze a scheduler-local terminal snapshot**

The snapshot contains only:

```text
attempt_id, frozen mode, generation, terminal result/cause,
T0/tick/HARD timestamps,
eligible BODY count, canonical V1 digest, last BODY key/time,
coverage state, invariant/lineage/proof counters
```

It contains no target id set, token, transfer epoch owner, callback, support edge, permit, or mutable ledger.

- [ ] **Step 3: Capture existing pipeline milestones once**

In the source owner only, use these baseline anchors without moving them:

```text
accepted source attempt:
  immediately after g_active_drain_attempt = candidate / active_drain_attempt
policy start:
  immediately before the readiness seam, as specified in Task 8
CLOSING:
  existing closing_command_gate_published release store
FINAL ACK:
  capture the existing final_ack_us in both control-only and normal success paths
phase2 end:
  every existing publish_phase2_metrics() completion updates diagnostics-only
  phase2_end_monotonic_us = phase2_total_started_us + phase2_metrics.total_us
```

Allocate `attempt_id` and the diagnostics context, including the frozen attempt `mode`, only after the source attempt is accepted. Integrate final emission into or immediately beside the existing phase2-metrics cleanup guard: first perform its existing “publish if not already published” action, then emit exactly one `PRESERVE_PHASE2_FINAL_V1`, and let the earlier active-attempt cleanup run afterward by normal reverse guard destruction. Do not use the later metrics log's fresh `preserve_trx_monotonic_us()` as the formal phase2 end, and do not assume an intermediate `publish_phase2_metrics()` call is the final one.

Use a compact owner milestone enum for `first_failure_stage`; update it at existing major anchors and set the failure once through existing shared error/cleanup lambdas. An unclassified early failure records the current milestone plus native terminal status. Do not rewrite or consolidate the many existing returns, and never query scheduler state after CLOSING.

- [ ] **Step 4: Compute the two formal intervals from one attempt**

```text
strict_interval_us = phase2_end_us - policy_start_us
last_command_end_to_final_ack_us = final_ack_us - last_body_exit_us
```

Missing, duplicate, cross-attempt, incomplete, fallback, or negative ordering makes the validator fail; it never changes source behavior.

- [ ] **Step 5: Run all seven scheduler MTR groups**

Run each with `--retry=0`. Expected: one self-consistent final record per attempt and exact failure classification.

---

### Task 11: Enforce source isolation and protected-pipeline ZERO_DIFF

**Files:**
- Modify: `scripts/preserve_trx_lint_runner.py`
- Modify: the existing MTR source-lint entry that invokes this runner
- Test: all scheduler mode/source-lint MTRs

- [ ] **Step 1: Write RED source-guard expectations**

Add manifests for allowed integration functions/ranges and protected pipeline anchors. The check must reject scheduler/mode/held/support/permit facts in target, progress, worker, token, batching, flush/ACK, final-HWM, receiver, promotion, and cleanup sinks.

- [ ] **Step 2: Implement the two checks**

Add:

```text
phase2_scheduler_allowed_integration_surface_lint
phase2_scheduler_protected_pipeline_trace_lint
```

Freeze exact hashes for ZERO_DIFF files and normalized anchors from active progress through FINAL ACK. Allow only the readiness seam, command facade/gates, external teardown wrappers, MDL proof, and InnoDB proof.

- [ ] **Step 3: Add mandatory in-memory negative self-checks**

The runner must mutate an in-memory copy to verify detection of:

```text
active progress tuple change
pending progress tuple change
CLOSING/target/worker anchor reorder
scheduler fact injected directly into a protected sink
scheduler fact indirectly changing final_hwm_async_capable or pending targets
ZERO_DIFF file content change
scheduler diff outside an allowed range
```

The worktree is never modified by the self-check.

- [ ] **Step 4: Run source guards**

Run:

```bash
python3 scripts/preserve_trx_lint_runner.py \
  --repo-root . --output-dir /tmp/preserve-phase2-scheduler-source-lint
```

Expected: all production checks and all negative self-checks pass.

- [ ] **Step 5: Recheck protected hashes**

Run the baseline `shasum` command from Task 1. Expected: every protected hash remains unchanged.

---

### Task 12: Add scheduler E2E and full-pressure profiles

**Files:**
- Create: `scripts/preserve_trx_phase2_scheduler_e2e.py`
- Modify: `scripts/preserve_trx_full_pressure_runner.py`
- Reuse unchanged: `scripts/resumable_trx_business_e2e.py`

- [ ] **Step 1: Add the three scheduler-specific E2E scenarios**

Implement only:

```text
mode-smoke
lock-ddl-source-restore
sysbench-write-only-drain
```

The script starts workloads and DRAIN, parses business/receiver/final-record facts, and emits JSON. It does not implement transfer protocol, receiver state machines, token cleanup, or RESET.

- [ ] **Step 2: Implement the final-record metamorphic self-check**

In memory, verify rejection of missing/duplicate records, cross-generation joins, invalid eligible digest/count/key, illegal coverage/fallback, old-tail substitution, and success milestones borrowed from another attempt. Emit `oracle_self_check` results into each report.

- [ ] **Step 3: Add two runner evidence profiles**

Extend the existing runner choices with:

```text
dependency-sysbench
dependency-mixed-transfer
```

The runner owns only isolated directories, ports, build identity, five-run orchestration, evidence indexing, and heavy-data cleanup. It delegates the workload to the scheduler adapter or existing mixed-transfer E2E.

- [ ] **Step 4: Run check-only validation**

Run:

```bash
python3 scripts/preserve_trx_full_pressure_runner.py \
  --profile full --evidence dependency-sysbench --check-only
python3 scripts/preserve_trx_full_pressure_runner.py \
  --profile full --evidence dependency-mixed-transfer --check-only
```

Expected: both print exact release-build/workload/evidence/cleanup checklists without creating datadirs.

- [ ] **Step 5: Run scheduler E2E smoke**

Run the new script's `mode-smoke` and `lock-ddl-source-restore` scenarios against the Debug binary. Expected: default dependency mode, one forced pre-transfer source restore, one successful drain, both servers alive, exact final records, and no cross-generation facts.

---

### Task 13: Build, regression, and performance acceptance

**Files:**
- No planned production edits; any failure returns to the owning task and requires a new RED-to-GREEN cycle.

- [ ] **Step 1: Run formatting and diff checks**

Run:

```bash
git diff --check
git status --short -uall
```

Expected: no whitespace errors and no unrelated changes.

- [ ] **Step 2: Build Debug and existing GUnit binaries**

Run:

```bash
cmake --build build-debug --target mysqld \
  preserve_trx-t preserve_trx_drain-t preserve_trx_warmcopy-t \
  preserve_trx_temp_table-t -j8
./build-debug/runtime_output_directory/preserve_trx-t --gtest_brief=1
./build-debug/runtime_output_directory/preserve_trx_drain-t --gtest_brief=1
./build-debug/runtime_output_directory/preserve_trx_warmcopy-t --gtest_brief=1
./build-debug/runtime_output_directory/preserve_trx_temp_table-t --gtest_brief=1
```

Expected: build exit 0 and all existing tests pass; no new Unit/GUnit was added.

- [ ] **Step 3: Run the three MTR profiles**

Assemble/run sequentially with separate vardirs and `--retry=0`:

```text
baseline-legacy:
  use the immutable pre-edit evidence and baseline binary SHA captured in Task 1
new-legacy:
  new binary, identical manifest, explicit LEGACY_READINESS_THEN_CLOSING
new-dependency:
  new binary, seven scheduler groups plus all applicable no-bin/log-bin/standby-transfer tests,
  explicit --mysqld=--rds-preserve-trx-standby-phase2-scheduler-mode=DEPENDENCY_CONVERGENCE_V1
  (or an equivalent isolated profile cnf)
```

The new-dependency runner must assert `mode=DEPENDENCY_CONVERGENCE_V1` from each attempt's final record; the mode test also verifies the runtime global. This prevents the suite-level legacy default from producing a false GREEN. Record binary SHA, exact manifest, injected options, pass/fail/skip, and shutdown-report separation. Any unexplained skip or missing mode oracle is failure.

- [ ] **Step 4: Re-run existing Preserve/Transfer/Receiver E2E in dependency mode**

Use the existing `resumable_trx_business_e2e.py` scenarios for successful handoff, send-before failure/source restore, `NOT_COMMITTED_CLEAN`, ACK uncertainty/`COMMIT_UNKNOWN`, receiver duplicate/conflict, NOT_READY/CORRUPT, cleanup debt, authoritative cohort, pending timeout/exclusion, detach, final fence, and all-or-nothing reattach. Do not duplicate these state machines in the scheduler script.

- [ ] **Step 5: Build Release**

Run:

```bash
cmake --build build-release --target mysqld -j8
```

Expected: exit 0. Record `shasum -a 256 build-release/runtime_output_directory/mysqld`.

- [ ] **Step 6: Run five formal sysbench rounds**

Run:

```bash
python3 scripts/preserve_trx_full_pressure_runner.py \
  --profile full --evidence dependency-sysbench
```

Expected for all five rounds: 1000 threads, 128 tables, 20,000 rows/table, 300 seconds steady run, no reconnect, successful DRAIN and receiver epoch, `strict_interval_us <= 2000000`, exact eligible BODY coverage, and zero scheduler fatal/invariant/lineage/proof errors.

- [ ] **Step 7: Run formal mixed standby-transfer pressure**

Run:

```bash
python3 scripts/preserve_trx_full_pressure_runner.py \
  --profile full --evidence dependency-mixed-transfer
```

Expected: successful handoff; exact `last_command_end_to_final_ack_us < 500000`; receiver READY after final-spool ACK `< 500000`; all receiver survivors READY; read load, QPS/P99, backlog, memory, residency, batch reduction, prewarm, and online-service gates pass.

- [ ] **Step 8: Produce the final evidence summary without committing**

Report:

```text
source HEAD and binary SHA
exact modified/created files and line counts
protected hash comparison
Debug build and existing GUnit results
three MTR profile manifests/results
dependency E2E results
five sysbench reports
mixed full-pressure report
remaining failures or unverified gates
```

Do not call the implementation complete unless every required command has fresh passing output.

---

### Task 14: Add the continuous large/short-transaction standby-transfer pressure profile

**Files:**
- Create: `scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py`
- Modify: `scripts/preserve_trx_full_pressure_runner.py`
- Modify: `scripts/resumable_trx_business_e2e.py`
- Modify: `Docs/superpowers/specs/2026-08-26-preserve-trx-phase2-command-scheduler-design.md` (already completed in design 11.5)
- Do not modify: `sql/**`, `storage/innobase/**`, transfer/receiver code, MTR, Unit/GUnit, or `scripts/tests/**`

- [ ] **Step 1: Create the dedicated Python E2E entry and capture RED**

Create the thin entry first, before adding runner support:

```python
#!/usr/bin/env python3

import sys

from preserve_trx_full_pressure_runner import main


if __name__ == "__main__":
    raise SystemExit(
        main(
            sys.argv[1:],
            forced_evidence="dependency-continuous-large-tx-transfer",
        )
    )
```

Run:

```bash
python3 scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py \
  --profile smoke --check-only --business-run-before-drain 3
```

Expected RED: runner rejects the unknown evidence/argument. This proves the new E2E entry reaches the missing profile rather than an existing workload.

- [ ] **Step 2: Add only the outer runner profile and make check-only GREEN**

Extend `FullPressureProfile` with zero-defaulted fields:

```python
short_transaction_sessions: int = 0
short_transaction_tables: int = 0
short_transaction_rows_per_table: int = 0
continuous_min_eligible_body_count: int = 0
continuous_min_record_locks: int = 0
```

Define full and smoke profiles by `dataclasses.replace()`:

```python
DEPENDENCY_CONTINUOUS_LARGE_TX_FULL_PROFILE = dataclasses.replace(
    FULL_PROFILE,
    name="dependency-continuous-large-tx-full",
    lockset_batch_size=10_000,
    business_run_before_drain_s=300.0,
    short_transaction_sessions=100,
    short_transaction_tables=50,
    short_transaction_rows_per_table=20_000,
    continuous_min_eligible_body_count=900,
    continuous_min_record_locks=5_000_000,
    scheduler_strict_interval_limit_us=2_000_000,
    source_post_command_tail_limit_us=500_000,
    ready_after_final_spool_ack_limit_us=500_000,
    preserve_memory_budget_bytes=2 * 1024**3,
)

DEPENDENCY_CONTINUOUS_LARGE_TX_SMOKE_PROFILE = dataclasses.replace(
    SMOKE_PROFILE,
    name="dependency-continuous-large-tx-smoke",
    statements_per_tx=1_000,
    seed_rows_per_table_per_session=1_000,
    lockset_batch_size=100,
    business_run_before_drain_s=3.0,
    short_transaction_sessions=4,
    short_transaction_tables=2,
    short_transaction_rows_per_table=1_000,
    continuous_min_eligible_body_count=1,
    continuous_min_record_locks=1,
    scheduler_strict_interval_limit_us=5_000_000,
    source_post_command_tail_limit_us=2_000_000,
)
```

Add `dependency-continuous-large-tx-transfer` to the evidence choice and dependency-mode set. Add an outer `--business-run-before-drain` float override; reject it for every other evidence, reject non-positive values, and replace only the selected new profile. Build the delegated E2E command with:

```text
--continuous-business-through-drain
--business-run-before-drain <profile value>
--short-transaction-sessions <profile value>
--short-transaction-table-count <profile value>
--short-transaction-rows-per-table <profile value>
--allow-partial-tokens
```

Keep the existing lockset flags, source/receiver commands, transfer parameters and receiver-read-load parameters unchanged. Add an acceptance-contract branch that declares 1000+100 sessions, no pre-DRAIN checkpoint, direct control DRAIN, the configured timing/lock minima, dependency mode and optional post-hoc lock-wait coverage.

Re-run the Step 1 command. Expected GREEN: exact check-only checklist is produced without creating datadirs, and it shows full dependency mode plus the smoke override of 3 seconds.

- [ ] **Step 3: Capture runtime RED before adding the inner workload**

Run the new smoke profile against the existing Release binary:

```bash
python3 scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py \
  --profile smoke --business-run-before-drain 3 \
  --required-free-gib 2 --skip-build
```

Expected RED: `resumable_trx_business_e2e.py` rejects the new continuous/short-transaction flags or lacks the new report contract. Preserve the report/log path as RED evidence.

- [ ] **Step 4: Add the isolated short-transaction workload and direct-DRAIN window**

Add these `HarnessConfig` fields and CLI flags with zero/false defaults:

```python
continuous_business_through_drain: bool = False
short_transaction_sessions: int = 0
short_transaction_table_count: int = 0
short_transaction_rows_per_table: int = 0
```

Validation for the new flag is exact: standby-transfer receiver scenario, positive business window, positive lockset batch size, an even short-session count, `short_transaction_table_count * 2 == short_transaction_sessions`, and at least 8 rows per short table. Other scenarios retain their old validation and flow.

Create short tables named `rtx_short_lock_000` through the configured count with an `id` primary key plus mutable `k`, `c`, and `pad` columns. Seed the configured row count in bounded 1000-row INSERT batches. These table names and rows are disjoint from every large-lockset table.

Add one `ShortTransactionWorker` per short session. Map two adjacent session ids to one table. Both peer workers execute the same lock order on one persistent connection:

```sql
START TRANSACTION;
UPDATE rtx_short_lock_N SET k=k+1 WHERE id=1;        -- shared gate first
UPDATE rtx_short_lock_N SET k=k+1 WHERE id=:private_update_id;
UPDATE rtx_short_lock_N SET c=:value WHERE id=:private_update_id;
DELETE FROM rtx_short_lock_N WHERE id=:private_delete_id;
INSERT INTO rtx_short_lock_N(id,k,c,pad) VALUES (...);
COMMIT;
```

Each role selects private ids from a disjoint half of its table. There is no pair coordinator, no sleep, no lock-wait query and no DRAIN/scheduler input. Because every transaction takes the shared gate before private rows, waits are one-way and deadlock-free. Record transaction elapsed microseconds and completed transaction count.

On 4020, set a per-worker `waiting_after_4020` event and wait on the existing global `stop_event` while retaining the original connection object. Do not reconnect. Any non-4020 error before stop is published to the existing worker error queue.

For the continuous profile only, change standby-transfer orchestration to this order:

```text
finish setup and obtain receiver baseline samples
start large and short workers
passively wait for every large worker to finish one range UPDATE
passively wait for every short worker to finish one transaction
capture T_business and checkpoint_generation_before_drain=0
sleep/check worker errors until the configured monotonic deadline
capture end progress and all 1100 worker liveness
switch the existing receiver probe to transfer measurement
immediately call the existing _execute_drain_preserve() once
```

Do not call `request_drain_checkpoint()`, pause, hold, `data_lock_waits`, tiered probe, or another source control statement anywhere in that window. Existing profiles keep their current ordering and checkpoint behavior byte-for-byte.

Replace the mixed-only survivor handoff with a neutral `standby_transfer_survivor_count` field used by both mixed and continuous partial-token profiles. Do not alter token selection or receiver readiness logic.

Add report fields for requested/actual window, start/deadline/DRAIN timestamps, checkpoint generation, start/end progress for both workloads, pre-DRAIN liveness, short transaction latency summary, 4020 count, workers waiting with original connections, neutral survivor count, and `lock_wait_coverage` derived only after DRAIN from scheduler summary (`OBSERVED` when `support_edge_registered>0`, otherwise `NOT_OBSERVED`). Zero edges are report-only and are not a functional/performance failure.

- [ ] **Step 5: Add the isolated report validator without weakening old profiles**

Add `validate_continuous_large_tx_report()` and dispatch only the new evidence to it. Require:

```text
effective mode == DEPENDENCY_CONVERGENCE_V1
1000/100 large workers and 100/4 short workers match the profile
all workers reached readiness and remained alive at DRAIN
actual business window >= requested window
large and short completed counters increased during the window
checkpoint_generation_before_drain == 0
harness_checkpoint_before_drain == false
one DRAIN call from independent_control_connection
drain success and both servers alive
survivor_count > 0
receiver READY == survivor_count and NOT_READY == 0
record locks >= profile minimum
eligible BODY >= profile minimum
all observed 4020 workers remain waiting on their original connection
strict interval and both 500ms-class limits satisfy the profile
existing memory/batch/prewarm/read-load/cleanup invariants remain enforced
```

Read `support_edge_registered` from the already validated attempt-matched scheduler summary. Emit `OBSERVED` for positive edges and `NOT_OBSERVED` for zero; do not fail solely for zero. Continue to fail on scheduler fatal, identity/proof unknown, invariant violations, malformed counts or cross-attempt joins.

- [ ] **Step 6: Run smoke GREEN and verify process cleanup**

Re-run the Step 3 command. Expected: one successful DRAIN, dependency final record, receiver READY completeness, all configured workers started, at least 3 complete business seconds, no checkpoint generation, no reconnect, and no leftover mysqld/E2E process. `lock_wait_coverage` may be either valid enum value.

Run the smoke a second time with an explicit one-second override:

```bash
python3 scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py \
  --profile smoke --business-run-before-drain 1 \
  --required-free-gib 2 --skip-build
```

Expected: the report records requested=1 second and actual>=1 second; DRAIN remains direct and single-shot.

- [ ] **Step 7: Verify scope and syntax before full pressure**

Run:

```bash
python3 -m py_compile \
  scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py \
  scripts/preserve_trx_full_pressure_runner.py \
  scripts/resumable_trx_business_e2e.py
git diff --check
git status --short -uall
git diff -- sql storage/innobase
```

Expected: Python compilation and diff check exit 0; this task has no new SQL/InnoDB diff, no test-unit diff and no unrelated file change. Review the existing 552-line runner delta separately so the new hunk does not overwrite prior scheduler evidence work.

- [ ] **Step 8: Run the formal 300-second full profile**

After confirming free space, run without `--skip-build`:

```bash
python3 scripts/preserve_trx_continuous_large_tx_full_pressure_e2e.py \
  --profile full
```

Expected: 1000 large plus 100 short workers run for a complete post-readiness 300 seconds; one time-driven DRAIN succeeds; 4020 connections remain held; receiver survivor completeness, strict 2-second interval, exact command-to-FINAL-ACK `<500000us`, FINAL-ACK-to-READY `<500000us`, five-million-lock minimum and all inherited pipeline gates pass. Report lock-wait coverage truthfully without changing pass/fail solely for `NOT_OBSERVED`.

- [ ] **Step 9: Report evidence without committing**

Report the exact new/modified files, RED and GREEN commands, binary SHA, workload start/deadline/DRAIN timestamps, both workload progress/latency, scheduler/lock coverage, survivor/receiver counts, Phase2 and double-500ms metrics, cleanup result, and any remaining failure. Do not stage, commit or push.

## Plan self-review checklist

- [ ] Every design invariant maps to at least one task and runtime/source guard.
- [ ] No task modifies transfer, receiver, promotion, drain orchestration, SQL prepare, InnoDB acquire/release, or an existing warmcopy method; the only warmcopy delta is the exact additive candidate-local live-fence setter from design 2.5.1.
- [ ] PS gate remains after successful precheck and before `mysqld_stmt_execute()`.
- [ ] T0 capture is active before T0; attempt route is not.
- [ ] External T0/tick never reads plain `trx_t::version` under only `LOCK_thd_data`; exact identity comes from native-safe or owner-thread producers.
- [ ] Every tick owns a `probe_inflight` borrow before using raw THD, and terminal/teardown cannot release its pin early.
- [ ] Every T0-active transaction reserves `PENDING_T0_ACTIVE_IDENTITY`; native-safe sealing is not ordinal creation.
- [ ] Dependency batch target/pending-boundary validation uses the same `OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT` SQL-active definition as T0; Legacy/OFF, session-only, phase1 prebuild and downstream pipeline remain unchanged.
- [ ] Exact T0_EXECUTING `PENDING_T0_BODY_FIRST_TX` late adoption is the only post-snapshot old-ordinal creation path.
- [ ] HELD waits release the scheduler mutex, are 5ms interruptible, and revalidate MDL demand/support/HARD on every bounded poll without per-connection timed condition waits.
- [ ] Normal HARD does not wait for 4020 acknowledgment；4020 在完整 native dispatch cleanup 之后、下一次 command-read 之前等待 latch，且在 non-survivor prune 前不可见。
- [ ] A post-CLOSING response-producing native command cannot bypass the scheduler latch; it waits on the same dependency-only classification/pin barrier, while no-response cleanup remains native.
- [ ] Normal/deadline HARD explicitly cuts off all pre-BODY stages; support loss explicitly returns support-dependent reserved permits to HELD.
- [ ] COM_CLONE defers common retirement only after successful post-response delegation.
- [ ] CLOSING publication, authoritative target classification, and exact lifetime-pin acquisition precede 4020 response release; response release precedes route retirement, with no nested attempt/route mutex.
- [ ] MDL probing is try-only and duration-aware in Release builds.
- [ ] RC/RU record S and X are both conservatively denied.
- [ ] InnoDB complete-set replacement and MDL demand-generation merge are not conflated.
- [ ] Teardown publishes the existing tombstone before entering scheduler teardown.
- [ ] No Unit/GUnit or Python unit-test file is added or modified.
- [ ] No commit step appears in this plan.
- [ ] Baseline evidence is frozen before edits, and every dependency profile proves its actual runtime mode.
