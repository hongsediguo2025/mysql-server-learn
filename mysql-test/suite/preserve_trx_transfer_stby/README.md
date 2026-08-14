# Preserve Trx Standby Transfer MTR

This suite contains standby-transfer T1 protocol MTRs. Two-mysqld cases prove
wire transfer, receiver process state, cleanup, and source-side data behavior.
Self-loop cases keep the same InnoDB pages available on both ends of the
wire so a narrow final-fact branch can be exercised deterministically. Neither
topology proves physical-standby SQL visibility, promotion adoption, or
dual-ownership safety.

Every business table in this suite is explicitly `ENGINE=InnoDB`. Each case
must establish a multi-statement transaction and independent committed
background work before invoking DRAIN. A new file is added only when it covers
a production branch or failure lifecycle not already exercised by
`suite/preserve_trx`; otherwise the existing test is augmented instead.

| Scenario | Evidence | Test/result | Config | Workload and assertions | Source branch | Status |
|---|---|---|---|---|---|---|
| X0-06 | T1 two-mysqld outage/restart lifecycle | `t/xfer2_receiver_unavailable_w04.{test,result}` | `t/xfer2_receiver_unavailable_w04.cnf` | W04: 32 accounts, balanced ledger, nonempty RR ReadView, outsider commit, three owner updates plus ledger writes, continued owner DML, a real lock waiter, and unrelated receiver-native data across restart | mysqld.2 is cleanly stopped before DRAIN; OPEN fails with 2003 before any receiver command, while the original THD retains its ReadView, writes, and lock and resolves fully before receiver restart; old-generation admission is excluded and the new generation is empty | passing |
| X0-07 | T1 two-mysqld receiver-generation lifecycle | `t/xfer2_receiver_restart_after_success_w01.{test,result}` | `t/xfer2_receiver_restart_after_success_w01.cnf` | Two independent W01/O01/O02 lifecycles plus unrelated receiver-native InnoDB data; the first transfer reaches COMMITTED/READY, then the source process is restarted and a second owner is drained while mysqld.2 is down | the first receiver generation retains exactly one accepted READY epoch without source replay; the second OPEN fails boundedly before any Phase1 frame while that owner retains its RR ReadView, writes, and lock; receiver restart clears old process-local epoch/prepared/cache state while native data survives | passing |
| X0-09 | T1 two-mysqld | `t/xfer2_receiver_feature_off_w04.{test,result}` | `t/xfer2_receiver_feature_off_w04.cnf` plus receiver-only `-slave.opt` | W04: 32 accounts, balanced ledger, nonempty RR ReadView, outsider commit, four owner DML operations, lock waiter, and an unrelated receiver-native transaction | source transfer is ON while receiver Preserve is OFF; authenticated COM receives 1047 before admission, the receiver frame/byte/debt/prepared/cache/memory ledgers stay at baseline, and the original owner retains its old ReadView and locks | passing |
| X1-01/X1-02 | T1 two-mysqld endpoint-admission matrix | `t/xfer2_endpoint_incomplete_w01.{test,result}` | `t/xfer2_endpoint_incomplete_w01.cnf` | Three independent W01/O01/O02 lifecycles cover missing host, missing port, and missing credential name; each has 32 accounts, a nonempty RR ReadView, outsider commit, three owner updates, one audit row, continued owner DML, a real lock waiter, exact rollback data, and unrelated receiver-native DML | every branch proves exactly one intended endpoint field is empty/zero while the others are valid; the batch-entry artifact decision returns `ER_PRESERVE_TRX_UNSUPPORTED` before readiness sampling, manager-state publication, TCP connect, or receiver admission, so the same owner retains its old ReadView, writes, and lock and the credential secret is never logged | passing |
| X1-03/X1-04/X1-05 | T1 two-mysqld credential-file matrix | `t/xfer2_credential_file_missing_w01.{test,result}` | `t/xfer2_credential_file_missing_w01.cnf` | One W01/O01/O02 transaction spans four labeled variants: missing path, exact 0644 mode, empty 0600 file, and exact 4097-byte 0600 file; 32 accounts, nonempty RR ReadView, outsider commit, three owner updates plus three audit rows, continued owner DML, a real same-record lock waiter, explicit rollback, and restart verification | each distinct local file predicate fails before TCP connect/admission; immediately after every failure the owner still sees its three writes and old RR version while the waiter stays blocked; only the owner rollback releases the waiter, whose update persists exactly once, and the final residue/restart checks prove no partial transfer lifecycle side effect | passing |
| X1-06 | T1 two-mysqld security contract | `t/xfer2_auth_wrong_password_w01.{test,result}` | `t/xfer2_auth_wrong_password_w01.cnf` | W01/O01/O02 with 32 accounts, RR/outsider visibility, three owner updates, three audit rows, continued owner DML, a real same-record lock waiter, exact commit data, and receiver-native DML | secure credential file is read, TCP authentication fails with user-bound 1045, receiver accounting stays unchanged, owner self-writes and old RR view survive while the waiter remains blocked, the owner commit releases that waiter exactly once, and the supplied wrong secret is absent from both error logs | passing |
| X1-07 | T1 two-mysqld | `t/xfer2_auth_missing_privilege_w01.{test,result}` | `t/xfer2_auth_missing_privilege_w01.cnf` | W01 with 32 accounts, RR/outsider visibility, three owner updates, audit insert, a real lock waiter, continued owner DML, exact commit data, and receiver-native DML | authentication succeeds but the dynamic transfer privilege check rejects with 1227 before frame admission; receiver accounting remains unchanged, the credential is not logged, and the source transaction retains its old RR view and locks | passing |
| X1-08 | T1 debug-contract two-mysqld | `t/xfer2_privilege_revoked_after_phase1_w01.{test,result}` | `t/xfer2_privilege_revoked_after_phase1_w01.cnf` | W01/O01/O02 with 32 accounts, nonempty RR ReadView, three nonadjacent owner updates, one audit row, outsider commit, a real lock waiter, and unrelated receiver-native DML across restart | after final metadata and asynchronous prewarm have produced real staged payload but before COMMIT, the receiver account loses `PRESERVE_TRX_TRANSFER_ADMIN`; the terminal COMMIT path receives 1227 with zero post-revoke frame/byte/terminal admission, source enters observable `COMMIT_UNKNOWN` quarantine, RESET is TOO_LATE, owner and outsider stay fenced, receiver staging survives regrant but clears on receiver restart, and source restart leaves only the outsider commit | passing |
| X1-09 | T1 debug-contract two-mysqld receiver-identity lifecycle | `t/xfer2_receiver_nonce_mismatch_w01.{test,result}` | `t/xfer2_receiver_nonce_mismatch_w01.cnf` | W01/O01/O02 plus unrelated receiver-native InnoDB data; source pauses after the process-bound OPEN ACK and before DECLARE_TOKEN/any Phase1 frame, then mysqld.2 restarts | the old process nonce is rejected in the new receiver generation before admission, staging, prepared/cache publication, debt, or worker activity; the same source owner retains its RR ReadView, writes, and lock and resolves exactly once, while the generation fingerprint changes without exposing the raw nonce | passing |
| X2-04 | T1 debug-contract two-mysqld exact Phase1 replay | `t/xfer2_phase1_duplicate_frame_retry_w01.{test,result}` | `t/xfer2_phase1_duplicate_frame_retry_w01.cnf` | W01/O01/O02 plus a real 6 MiB logged W08B object split across multiple chunks and Phase1 batches; this passing W08B increment does not satisfy the family-mandatory W08A binding | after one non-final OBJECT_CHUNK payload is semantically applied and its reservation released, the positive ACK is suppressed once; the source reconnects and replays the byte-identical payload, receiver sequences report `ALREADY_APPLIED`, admitted/applied bytes are not duplicated, later seal/COMMIT succeeds, and data plus process resources reconcile exactly | runtime increment passing; base catalog ID unclosed (mandatory W08A capability blocker) |
| X2-14 | T1 debug-contract two-mysqld connection-loss lifecycle | `t/xfer2_phase1_sender_connection_loss_w01.{test,result}` | `t/xfer2_phase1_sender_connection_loss_w01.cnf` | W01/O01/O02/O10 plus a real 6 MiB logged W08B object spanning multiple non-final chunks; this passing W08B increment does not satisfy the family-mandatory W08A binding | the receiver closes the real data connection only after a non-final OBJECT_CHUNK payload is applied but before its ACK; exact uncertain-payload replay is idempotent, remaining chunks and seal complete, no partial READY or duplicate object bytes are published, and DRAIN/data/cleanup finish once | runtime increment passing; base catalog ID unclosed (mandatory W08A capability blocker) |
| X5-11a | T1 two-mysqld | `t/xfer2_multi_statement_w01.{test,result}` | `t/xfer2_multi_statement_w01.cnf` | W14: 32 accounts, nonempty RR ReadView, outsider commit, three owner updates and audit insert; source restart proves owner rollback and outsider durability | valid nonempty ReadView passes source strict admission and crosses the wire through receiver COMMIT; the unmatched physical topology then closes as `PREWARM_DEADLINE`, not as a claimed ReadView incompatibility | passing |
| X5-11b | T1 self-loop | `t/xfer1_read_view_ready_w14.{test,result}` | `t/xfer1_read_view_ready_w14.cnf` | W14 double-entry transfer with three owner updates, audit rows, and an outsider commit | valid nonempty ReadView reaches authenticated final-fact bind and process-local READY | passing |
| X5-11c | debug-contract self-loop | `t/xfer1_read_view_horizon_failclosed_w14.{test,result}` | `t/xfer1_read_view_horizon_failclosed_w14.cnf` | W14 plus the same outsider visibility boundary; one typed not-ready classification, no READY bind, memory/prepared/cache/inflight resources return to baseline | authenticated final-fact ReadView horizon rejection and token-local staging cleanup | passing |
| X5-12a | debug-contract self-loop | `t/xfer1_partial_deadline_ready_retention_w14x3.{test,result}` | `t/xfer1_partial_deadline_ready_retention_w14x3.cnf` | Three independent W14 transactions over 32 accounts; 2 READY and 1 delayed token at cutoff; one selected-token cleanup failure is retried | selected READY publications are renewed beyond the old prewarm cutoff, the failed token becomes exact `PREWARM_DEADLINE`, cleanup debt completes once, and inflight/staged-capacity state returns to baseline; this is not the full four-reason W16 matrix or a TTL-expiry test | passing |
| X5-12b | debug-contract self-loop | `t/xfer1_cleanup_tainted_online_retry_w14x2.{test,result}` | `t/xfer1_cleanup_tainted_online_retry_w14x2.cnf` | Two independent W14 transactions with three owner DML operations each and outsider visibility checks; one READY sibling and one selected failed token | five actual post-selection staging-cleanup failures reach `CLEANUP_TAINTED`; bounded low-frequency retry then clears the debt online without invalidating the READY sibling | passing |
| X6-02 | T1 debug-contract two-mysqld terminal duplicate | `t/xfer2_terminal_duplicate_commit_w01.{test,result}` | `t/xfer2_terminal_duplicate_commit_w01.cnf` | W04/O01/O02/O10: debit id 3 by 40, credit id 7 by 40, append two balanced ledger rows, and keep a same-record waiter blocked; one process-bound COMMIT ACK is lost and the normal transport reconnects | the byte-identical COMMIT replay has one terminal CAS winner, exactly one duplicate, zero conflicts, and one original READY/prepared/cache publication; the O02 closing exclusion is the only failed receiver token; the W04 branch passes, but the duplicate-frame family binding additionally requires W08A | runtime increment passing; base catalog ID unclosed (mandatory W08A capability blocker) |
| X6-03 | T1 debug-contract two-mysqld terminal conflict | `t/xfer2_terminal_conflicting_commit_w01.{test,result}` | `t/xfer2_terminal_conflicting_commit_w01.cnf` | W04/O01/O02/O10 with the same balanced debit/credit and ledger; after the exact retry succeeds, a debug-only real transport probe keeps epoch/operation/sequence fixed and changes only the terminal fact digest | receiver records one duplicate then one conflict without replacing or corrupting the original COMMITTED result; a pinned-original authenticated status query remains stable, DRAIN returns that original result, and READY/prepared/cache plus data publish only once; the W04 branch passes, but the duplicate-frame family binding additionally requires W08A | runtime increment passing; base catalog ID unclosed (mandatory W08A capability blocker) |
| X6-12 | T1 terminal-only debug-contract two-mysqld | `t/xfer2_terminal_tombstone_expiry_w01.{test,result}` | `t/xfer2_terminal_tombstone_expiry_w01.cnf` | W04/O01/O02/O10 with a balanced debit/credit pair and ledger, two lost COMMIT ACKs, an authenticated live COMMITTED status query, and a bounded debug future-clock invocation of the real receiver reaper | the second byte-identical authenticated query runs after accepted/terminal retention expiry, returns NOT_FOUND, increments tombstone-expiry exactly once, and leaves active epoch, records, prepared/cache/memory, staging, debt, worker, and queue at baseline; source remains fail-closed in `COMMIT_UNKNOWN` until restart and no expired state is resurrected | passing; catalog binding closed |
| X6-08a | debug-contract two-mysqld | `t/xfer2_precommit_ack_uncertain_reset_w14.{test,result}` | `t/xfer2_precommit_ack_uncertain_reset_w14.cnf` | W14 with a nonempty RR ReadView, three owner updates, audit insert, and an outsider commit; final token metadata is delivered before its source-side result is made uncertain | pre-COMMIT uncertainty prevents `COMMIT_EPOCH` and every later cleanup frame, keeps the source owner fenced after DRAIN fails, and restores it only through explicit `RESET DRAIN`; receiver inflight state is unchanged by RESET and is cleared by receiver restart, not by an unimplemented online TTL | passing |
| X7-04a | debug-contract two-mysqld | `t/xfer2_precommit_abandon_reaper_retry_w14.{test,result}` | `t/xfer2_precommit_abandon_reaper_retry_w14.cnf` | W14 with three owner updates, audit insert, outsider commit, and real receiver staging before RESET | RESET wins before COMMIT admission; foreground ABANDON cleanup is deferred and the online receiver reaper completes the same authenticated ABANDONING cleanup without restart | passing |
| X7-05 | debug-contract two-mysqld | `t/xfer2_post_commit_reset_too_late_w14.{test,result}` | `t/xfer2_post_commit_reset_too_late_w14.cnf` | W14 with a nonempty RR ReadView, three owner updates, audit insert, and outsider commit; the source pauses after the independent receiver's process-bound COMMIT ACK and resumes through a normalized successful DRAIN | receiver accepted epoch, terminal CAS, tombstone, admitted-flow, and ACK timestamp prove COMMIT; RESET is TOO_LATE once in `HANDOFF_PENDING` and again after final arbitration reaches `COMMITTED_HANDOFF`; neither RESET emits a receiver frame and all original source sessions remain fenced | passing |
| X12-06 | T1 debug-contract two-mysqld foreground isolation | `t/xfer2_scale_native_foreground_w01.{test,result}` | `t/xfer2_scale_native_foreground_w01.cnf` | W01/O06/O09: DRAIN pauses after OPEN and the first token declaration while an ordinary non-target source transaction and an unrelated receiver-native transaction each update 64 InnoDB rows and commit | both foreground commits are visible before DRAIN resumes; only the original W01 token is admitted, classified, and made READY; source restart rolls back that preserved target while the source-native and receiver-native commits survive their respective restarts and the receiver table remains writable | passing; catalog binding closed |
| X12-07 | debug-contract two-mysqld | `t/xfer2_token_metadata_admission_w14x2.{test,result}` | `t/xfer2_token_metadata_admission_w14x2.cnf` | Two late-staggered W14 transactions, each with a nonempty RR ReadView, three owner updates, audit insert, and an outsider commit; source owners explicitly commit/rollback after rejection; token-ABORT ACK loss is armed | the first `DECLARE_TOKEN` holds an exact 1536-byte debug-extended metadata lease inside the receiver's 2048-byte budget; the late second declaration receives a process/channel-bound `RESOURCE_EXHAUSTED` response, consumes exactly its rejected sequence, marks the epoch precommit-poisoned, and sends the terminal precommit ABANDON before any best-effort token ABORT can occupy the data session; exactly two unique sequence-tracked frames are admitted, so no object/payload/COMMIT/ABORT frame is admitted; token metadata and sequence bookkeeping retire online while one bounded terminal tombstone retains retry status | passing |

X2-04, X2-14, X6-02, and X6-03 receive split credit. Their implemented
transport or terminal branches pass, but their base catalog IDs remain
unclosed because the direct design mandates W08A. A diagnostic no-bin probe
updated 768 rows containing 6000-byte `VARBINARY` values; it produced 1156
record locks but only one 31312-byte Phase1 batch, zero oversize tokens, and no
non-final `OBJECT_CHUNK` fault. On this production path persistent row bodies
are not exposed as transfer objects, so the existing W08B or W04 evidence
cannot be relabeled as W08A. The probe changes were reverted and add no MTR
file, result, or runtime label.

Closest existing coverage: `preserve_trx.standby_transfer_drain_no_shutdown`
uses one UPDATE and checks only basic epoch acceptance. X5-11a is retained only
for the formerly rejected nonempty-ReadView source/wire admission branch plus
its data disposition; its eventual zero-ready deadline is explicitly not
counted as a new failure reason. X5-11 adds a real nonempty RR ReadView, three
successive DML statements on one locked InnoDB record, per-statement effects,
and outsider data checks. It is not counted as another basic smoke scenario.

`preserve_trx.standby_transfer_gtid_on_strict_ready` already covers a normal
self-loop all-ready path without a nonempty RR ReadView. X5-11b adds the
ReadView creator/semantic/horizon contract and an independent multi-row data
background. X5-11c forces only the receiver horizon comparison to fail, leaves
the authenticated payload, epoch fact, and digest unchanged, and verifies
immediate classification plus complete resource/staging retirement.

X5-12a is retained because existing partial-selection coverage has either an
immediate 2/1 split or a deadline-driven 0/3 split. It adds the production
combination of a cutoff-time 2/1 partition, selected-only prepared-deadline
renewal, a late worker, and a real cleanup-debt retry. It does not claim that
the renewed retention TTL itself expires online. Each case instead proves its
intended pre-restart state, then verifies eleven process-local epoch, prepared,
cache, memory, worker, and queue gauges are zero in the new receiver generation.

X5-12b is not another deadline-classification case. It starts after the failed
token has already been selected, drives the real cleanup-debt retry budget to
`CLEANUP_TAINTED`, and proves that later online recovery releases the retained
payload and accounting debt. X6-08a differs from X7-04a: X7-04a has a
deterministic RESET winner and intentionally sends authenticated ABANDON,
whereas X6-08a has a debug-forced source-side pre-COMMIT uncertain result and proves
that both the failed DRAIN and later RESET remain source-local. The receiver
has no online timeout for this pure pre-COMMIT residue, so the case keeps it
visible until the existing restart cleanup establishes a new receiver
generation. This RESET contract ends at the COMMIT ownership barrier:
`HANDOFF_PENDING`, `COMMIT_UNKNOWN`, and committed handoff reject RESET as too
late; a local shutdown handoff with no COMMIT remains resettable. X12-07 is
not credited as object/payload-cap coverage: its limit is charged at
`DECLARE_TOKEN`, before BEGIN/chunk staging, and no existing runtime
object-size MTR is used to close those separate catalog rows. The response
binds the receiver process nonce, exact payload digest,
and sequence, but it is not a keyed signature and is not evidence of physical
standby ownership or an HA fence. The rejected epoch never reaches COMMIT.
`failed_tokens` returns to baseline before receiver restart; the retained
terminal tombstone is asserted separately and exists only for bounded response
replay. The source-side fault hook drops both responses for a token ABORT; the
case therefore fails if cleanup regresses to ABORT-first ordering and proves
that the epoch-wide terminal ABANDON is given transport priority. The
exact admitted-frame delta plus staged-memory baseline prove that rejection
happened before object or payload staging. Cleanup of an already staged payload belongs to the separate
ABANDONING-retry scenario and is not credited here.

X1-03/X1-04/X1-05 intentionally share one MTR file rather than cloning the
same W01 workload. The four labeled DRAIN attempts reach distinct credential
reader predicates (`open`, secure metadata, nonempty size, and the 4096-byte
maximum), and each has immediate owner/RR continuity plus O02 blocked-waiter
assertions. Receiver accounting is checked once across the complete pre-TCP
matrix because no variant can create a receiver session. The owner rollback,
not any failed DRAIN, releases the waiter; exact data and restart checks prove
that the waiter executes once and the owner writes remain rolled back.

X1-01/X1-02 similarly share one driver but keep three complete source
lifecycles. Every restart omits exactly one startup-only field and asserts the
other endpoint fields before creating W01/O01/O02. The global
`preserve_trx_transfer_artifact_decision()` is rejected by
`Preserve_trx_drain_service::execute()` before phase1 readiness sampling; the
later per-target check remains as a revalidation guard. Unchanged readiness,
source frame/send, receiver connection/admission/resource, and source
ownership facts jointly prove that an incomplete endpoint cannot partially
enter the drain or transfer lifecycle. For X1-02, the source call ordering
returns from credential-name readiness before credential snapshot/file-read;
the runtime assertions prove the secret marker is absent from both error logs,
not a separate file-open telemetry claim.

X1-08 is not a second X1-07. X1-07 denies the first authenticated transfer
command and therefore proves zero receiver staging plus immediate source
ownership retention. X1-08 first waits for final metadata, one registered
`PREWARMED_PENDING_FINAL_FACT` token, real staged payload, an idle receiver
worker, and an empty receiver queue. Dynamic privilege revocation then rejects
the terminal COMMIT before receiver admission. The source transport conservatively
classifies that explicit server error as ACK uncertainty, so the case proves
`COMMIT_UNKNOWN`, source quarantine, RESET TOO_LATE, and continuing 4020 fences
instead of claiming automatic source restoration. No cleanup command reaches
the receiver: its cleanup-debt/taint counters remain unchanged while inflight,
prepared, cache, and memory gauges retain the unresolved state until receiver
restart. Regrant permits unrelated receiver-native DML but does not silently
retry or clean the transfer. Source shutdown/restart finally rolls back the
fenced owner and aborts the waiter, leaving only the outsider update. This is T1
control/lifecycle evidence, not authoritative handoff resolution, physical
standby ownership, or an online orphan-TTL claim. Its I09 credit is limited to
restart invalidation of the observed process-local inflight, staging, prepared,
and cache state; it does not claim a promotion lease or an explicit nonce test.

X0-06 and X1-06 both reach the source client's
`mysql_real_connect()==nullptr` handling. X0-06 is therefore not credited as a
second source cleanup branch. Its independent increment is the real receiver
outage lifecycle: a cleanly stopped mysqld.2 yields exactly one 2003 failure,
no command send occurs, the source owner resolves while the receiver remains
down, and a later receiver restart preserves unrelated native InnoDB data
while exposing no transfer residue. X1-06 instead owns the online receiver
authentication/security contract for 1045.

Latest Debug gate:

```text
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx_transfer_stby --parallel=8 --force --retry=0
```

Result: all twenty-four MTR files passed without `--record`; together they
cover twenty-seven labeled runtime contracts. The suite shutdown report is
harness output and is not counted as a scenario. Before the full gate, the
X12-06 targeted `--record` and no-record runs passed, followed by the four-file
X0-07/X1-09/X12-06/X12-07 adjacent selection. The two Phase1 W08B fault cases
also passed after the compact stable-page prebuilt fix. The binding-qualified
catalog closure count is fifteen; X12-06 is the newly closed base ID. The
six-file X1 endpoint/credential/privilege/receiver-identity selection passed
at `--parallel=4`; the two changed O02 files also passed record and targeted
no-record execution. The three W04 terminal cases passed record and targeted
no-record execution, and the eight-test adjacent X6/X7 selection passed at
`--parallel=4`. Impacted existing
strict-ready, basic two-server drain, partial-selection, deadline,
global-failure, and remote RESET cases passed in targeted runs. No wait timeout,
false boolean, or `NULL` assertion is accepted as a golden result.
