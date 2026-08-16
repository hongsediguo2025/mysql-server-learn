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
| X7-01 / X10-01 / X10-09 | T1 debug-contract two-mysqld OPEN-only RESET and record waiter | `t/xfer2_reset_before_first_frame_w04.{test,result}` | `t/xfer2_reset_before_first_frame_w04.cnf` | W04/O01/O02/O09: a real same-record waiter is blocked before DRAIN; source pauses after authenticated `OPEN_EPOCH` ACK but before the first sequence-tracked frame, while unrelated receiver-native DML commits | RESET emits no invented DECLARE/ABORT token frame, sends one process-bound open-only ABANDON, creates exactly one `NOT_COMMITTED_CLEAN` CAS/tombstone, and leaves token/payload/prepared/cache/debt state empty; the same source owner and RR view return, the waiter remains blocked until explicit owner rollback, then commits exactly once; GUnit proves terminal retention reaps the bounded context without restart | targeted passing; augment-existing runtime credit, current fifty-six-file full gate passing |
| X7-02 ACK-loss increment | T1 debug-contract two-mysqld | `t/xfer2_phase1_abort_ack_loss_w04.{test,result}` | `t/xfer2_phase1_abort_ack_loss_w04.cnf` | W04/O01/O02/O09/O10 with a balanced debit/credit pair, ledger, old RR view, outsider commit, same-record waiter, and unrelated receiver-native commit; RESET wins after real Phase1 admission while both token-ABORT ACKs and the first successful epoch-ABANDON ACK are hidden | the byte-identical ABORT replay is sequence-idempotent; cleanup binds its exact digest to one epoch-wide ABANDON, then replays that same terminal operation after ACK loss; terminal CAS wins and tombstones each increase once, duplicates increase by the existing cleanup replay plus exactly one ABANDON ACK-loss replay, all receiver resources retire, and source ownership is restored without handoff/unknown/quarantine state | targeted passing; current fifty-six-file full gate passing |
| X7-04a | debug-contract two-mysqld | `t/xfer2_precommit_abandon_reaper_retry_w14.{test,result}` | `t/xfer2_precommit_abandon_reaper_retry_w14.cnf` | W14 with three owner updates, audit insert, outsider commit, and real receiver staging before RESET | RESET wins before COMMIT admission; foreground ABANDON cleanup is deferred and the online receiver reaper completes the same authenticated ABANDONING cleanup without restart | passing |
| X7-05 | debug-contract two-mysqld | `t/xfer2_post_commit_reset_too_late_w14.{test,result}` | `t/xfer2_post_commit_reset_too_late_w14.cnf` | W14 with a nonempty RR ReadView, three owner updates, audit insert, and outsider commit; the source pauses after the independent receiver's process-bound COMMIT ACK and resumes through a normalized successful DRAIN | receiver accepted epoch, terminal CAS, tombstone, admitted-flow, and ACK timestamp prove COMMIT; RESET is TOO_LATE once in `HANDOFF_PENDING` and again after final arbitration reaches `COMMITTED_HANDOFF`; neither RESET emits a receiver frame and all original source sessions remain fenced | passing |
| X9-02 | T1 debug-contract two-mysqld Phase2 failure data semantics | `t/xfer2_reset_during_phase2_w04.{test,result}` | `t/xfer2_reset_during_phase2_w04.cnf` | W02/W04/O01/O09/O10: 24 seeded inventory rows, one order with three FK-bound items, two allocated lines, one deleted pending line, status/sku secondary indexes, balanced account/ledger DML, an old RR view, outsider commit, and unrelated receiver-native commit | source pauses after every target is quiesced and `BATCH_DRAINING` is fenced but before final metadata/COMMIT; RESET retires every receiver Phase1 resource and restores the same THD with the exact order/items/inventory model, which then adds one item and one inventory delta before explicit rollback; outsiders see no half order or partial stock change, receiver-native data survives, and post-test dictionary allocators are aligned | targeted record/no-record passing; current fifty-six-file full gate passing |
| X3-01 / X9-03 | T1 debug-contract two-mysqld late final-HWM and exact retry | `t/xfer2_index_frame_retry_w03.{test,result}` | `t/xfer2_index_frame_retry_w03.cnf` | W03/O01/O09/O10: one logged RR owner changes all 2048 primary, nonunique-secondary, and unique-key rows; after a real non-final object payload is applied and replayed, the same owner rewrites 512 rows and 768 KiB of payload while an outsider remains on the committed baseline | Phase1 exact retry remains `ALREADY_APPLIED`; target-local synchronization proves the late delta enters the quiesced Phase2 final-HWM queue before terminal metadata; DRAIN and the byte-identical terminal retry succeed once, no receiver resource/debt remains, and source restart restores the exact primary/secondary/unique baseline including removal of every late key | targeted no-record passing; X3-01 augment-existing runtime binding added without a production change; formal X3-01 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X3-10 | T1 debug-contract two-mysqld prewarm/final-fact race | `t/xfer2_final_fact_prewarm_bind_race_w01.{test,result}` | `t/xfer2_final_fact_prewarm_bind_race_w01.cnf` | W01/O01/O09: one RR owner observes an old version, performs balanced indexed DML and audit inserts, and transfers into an exact dictionary-matched but data-empty receiver shadow; independent source and receiver-native transactions commit while the receiver has queued the owner's payload and source COMMIT is paused | strict receiver prewarm first reaches `PREWARMED_PENDING_FINAL_FACT` with one registered token but zero accepted epoch, classification, READY publication, or final-fact bind; after source COMMIT is released, terminal CAS, tombstone, bind attempt, classification, READY publication, prepared save, and active epoch each advance exactly once, all queues/debt return to baseline, receiver shadows remain empty, and native commits survive both restarts | targeted no-record passing; fifty-sixth triplet; no production change; current fifty-six-file full gate passing |
| X5-02 / X9-06 pre-final increment | T1 debug-contract two-mysqld token-local partition | `t/xfer2_token_local_partial_selection_w04x3.{test,result}` | `t/xfer2_token_local_partial_selection_w04x3.cnf` | Three independent W04 survivors plus one W06 source-local owner over 64 seeded rows; the W06 owner performs PK-range and secondary-predicate updates, deletes an exact four-row set, and keeps an old UPDATE executing across the closing deadline; an unrelated receiver-native transaction commits while all survivor payloads remain queued | closing excludes only the W06 owner and lets its already-running command finish, then fences its next command; receiver terminal admission and the existing one-token prewarm fault classify the three survivors as exactly 2 READY/1 NOT_READY, while receiver failed-token state accounts separately for the pre-final ABORT and post-COMMIT selection failure; source restart rolls back the excluded W06 transaction to the exact 64-row baseline and receiver-native data survives | targeted no-record passing; paired with the mandatory W16 post-COMMIT increment below; formal X9-06 closure is covered by the current fifty-six-file passing gate |
| X9-06 post-COMMIT increment / X12-05 | T1 debug-contract two-mysqld repeated W16 partition | `t/xfer2_repeated_epoch_retention_w16.{test,result}` | `t/xfer2_repeated_epoch_retention_w16.cnf` | Two source generations each run one exact W16 epoch: W01 compact update, W05 64-row insert/update/delete, W07 savepoint rollback, and W08B logged 256 KiB BLOB; O01 source observers and O09 receiver-native commits establish isolation | each epoch classifies exactly 1 READY/3 PREWARM_DEADLINE tokens, binds the last failed identity to its W08B owner, keeps every source owner fenced, retains three receiver failed records beside one saved/prepared READY sibling across source restart, and releases failed-token inflight/staging/debt; the second generation repeats the same partition without sequence/terminal collision and receiver restart clears both process-local epochs | targeted no-record passing; mandatory X9-06 W16 runtime binding present; formal X9-06 closure is covered by the current fifty-six-file passing gate |
| X7-04 / X9-07 / X10-02 | T1 debug-contract two-mysqld pre-CAS RESET and RR next-key semantics | `t/xfer2_reset_before_terminal_cas_w04.{test,result}` | `t/xfer2_reset_before_terminal_cas_w04.cnf` | Separate W04, W07, and W03/RR owners; W07 keeps S1 and post-S2 work while rolling back every S2 effect; W03 mutates primary, nonunique-secondary, and unique-key paths, then locks a real nonunique-index range while an adjacent INSERT waits inside its gap; committed source and receiver outsiders establish isolation | after all three final-token payloads are ACKed but before receiver COMMIT_EPOCH CAS, RESET wins one authenticated NOT_COMMITTED CAS plus one exact ABANDON duplicate and retires receiver resources without READY; all original THDs and exact transactional state return, the gap waiter remains in `data_lock_waits` after unrelated owner resolution, and only the restored W03 owner rollback lets it commit once; exact range/unique indexes and committed effects survive source restart | targeted no-record passing; X10-02 augment-existing runtime binding added without a kernel change; formal X9-07/X10-02 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X9-09 | T1 debug-contract two-mysqld temp-sidecar rejection | `t/xfer2_temp_sidecar_reject_w09.{test,result}` | `t/xfer2_temp_sidecar_reject_w09.cnf` | W09/O01/O06/O09: one owner mixes indexed INSERT/UPDATE/DELETE operations over 24 permanent and 24 temporary rows; independent source and receiver native transactions commit while DRAIN is paused after real OPEN/DECLARE admission | strict standby semantics reject the mixed engine shape before any portable object or terminal COMMIT; cleanup removes the already sealed image/undo sidecars, restores the original owner with both transaction halves exact and writable, and leaves no receiver publication or source ownership ambiguity; explicit owner rollback restores both baselines, while native commits survive both restarts | targeted no-record passing; formal X9-09 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X9-10 | T1 debug-contract two-mysqld RESET/retry data semantics | `t/xfer2_reset_during_phase1_w04.{test,result}` | `t/xfer2_reset_during_phase1_w04.cnf` | W04/W10/O01/O02/O09/O10: one RR owner mixes auto and explicit IDs, update/delete, balanced account/ledger DML, and a same-record waiter; an outsider consumes and commits ID 1202 and receiver-native DML commits while the first Phase1 is paused | RESET retires the first receiver payload and restores the same owner with its RR view, locks, writes, `LAST_INSERT_ID()=1201`, and reservations intact; the owner then receives 1203/1204, a fresh retry reaches COMMITTED/READY through an identity-matched but data-empty T1 shadow schema, source restart rolls back only owner effects, committed background rows survive, and the next actual auto ID is 1205 without reuse or duplicates | targeted record/no-record passing; current fifty-six-file full gate passing |
| X9-11 | T1 debug-contract two-mysqld pre-CAS RESET resolution | `t/xfer2_partition_collation_resolution_w11w12.{test,result}` | `t/xfer2_partition_collation_resolution_w11w12.cnf` | W11/W12/O01/O09/O10: one owner performs indexed INSERT/UPDATE/DELETE work across both physical RANGE partitions plus UTF8MB4 case/accent keys, binary keys, NULLable secondary keys, signed/unsigned BIGINT limits, and DECIMAL limits; outsider and receiver-native commits establish isolation | after all final-token payloads are admitted and sealed but before receiver ownership CAS, RESET wins with one NOT_COMMITTED terminal CAS and no conflict; receiver state retires without READY publication, the same source THD resumes, full/partition-pruned/secondary-index scans agree, continued owner DML commits once, and exact boundary values plus native commits survive both restarts; incidental reaper re-entry is covered only by dedicated retry tests | targeted no-record passing; formal X9-11 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X9-12 | T1 debug-contract two-mysqld isolation/GTID resolution | `t/xfer2_isolation_gtid_resolution_w13w14w15.{test,result}` | `t/xfer2_isolation_gtid_resolution_w13w14w15.cnf` | W13/W14/W15/O01/O09/O10: four disjoint source owners over 64 indexed rows and 12 audit rows cover RC statement visibility, one nonempty RR ReadView, one logged AUTOMATIC-GTID cache, and one `sql_log_bin=0` comparison; an independent source update and receiver-native transaction commit in the background | after every final-token payload is admitted and sealed but before receiver ownership CAS, P_S distinguishes exactly three session-off tokens, one logged-cache/AUTOMATIC token, three RC tokens, and one RR ReadView; RESET produces one NOT_COMMITTED CAS with no conflict, retires receiver state, restores all four original THDs and their session/cache state, lets the RC reader change only at legal commit, and allocates exactly source UUID GNO 1 only for the logged owner; all logical and index views survive both restarts; incidental reaper re-entry is covered only by dedicated retry tests | targeted no-record passing; formal X9-12 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X10-03 / X12-06 | T1 debug-contract two-mysqld RC and foreground isolation | `t/xfer2_scale_native_foreground_w01.{test,result}` | `t/xfer2_scale_native_foreground_w01.cnf` | W01/W03(RC)/O03/O06/O09: DRAIN pauses after OPEN and two token declarations; the RC owner performs deterministic primary/nonunique/unique-index DML and locks a 21-row nonunique-index range, while a non-target adjacent INSERT plus independent source and receiver transactions commit during the pause; an empty receiver shadow is bound to the exact source table/index/space fingerprint | the adjacent key commits without an RR-style gap wait, both frozen targets detach and become READY, and receiver shadows remain dictionary-only; source restart rolls back both owners but preserves the adjacent/source-native commits, while receiver restart preserves its native commit; exact aggregate, range, unique, deleted-row, and table checks all agree | targeted no-record passing; X10-03 augment-existing runtime binding added without a kernel change; prior X12-06 closure retained, while formal X10-03 closure and any new suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X10-04 | T1 debug-contract two-mysqld DDL/MDL failure lifecycle | `t/xfer2_alter_mdl_phase1_w02.{test,result}` | `t/xfer2_alter_mdl_phase1_w02.cnf` | W02/O04/O06/O09: one owner builds an exact FK-bound order/items/inventory transaction while a real `ALTER TABLE` requests EXCLUSIVE MDL after Phase1 OPEN; independent source and receiver transactions commit during the wait, and empty receiver shadows match source table/index/space identities | P_S proves the ALTER is PENDING behind the owner's granted MDL; closing rejects the epoch before detach because the waiting DDL itself has an unsupported active transaction, leaves the original owner live and the ALTER blocked, releases every receiver payload/prepared resource while retaining exactly two ABORTED diagnostics, then lets owner rollback release one successful ALTER; the DDL and native commits survive both restarts | targeted record-path/no-record business passing; no production change; formal X10-04 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
| X10-08 | T1 debug-contract two-mysqld overlapping-lock lifecycle | `t/xfer2_overlapping_token_contention_w16.{test,result}` | `t/xfer2_overlapping_token_contention_w16.cnf` | W16/O01/O07/O09/O10: W01 holds one shared record plus a balanced account transaction; W05 first completes a 64-row insert/update/delete model and then waits on that record; W07 retains S1/post-S2 state; W08B carries a logged 512 KiB cache; independent source and receiver commits establish isolation | Phase1 selects four candidates, the 5-second closing deadline removes only the W05 waiter and leaves exactly three detached survivor leases; RESET wins before terminal CAS, clean ABANDON retires receiver staging, and the original W01 rollback—not RESET—releases W05 to commit exactly once; W07 commits, W01/W08B roll back, and exact data/index state survives both restarts | targeted record-path/no-record business passing; no production change; formal X10-08 closure and any suite-wide passing claim are covered by the current fifty-six-file passing gate |
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

Last recorded complete Debug gate before the current suite expansion:

```text
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx_transfer_stby --parallel=8 --force --retry=0
```

At that checkpoint, all twenty-four MTR files passed without `--record`; together they
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
false boolean, or `NULL` assertion is accepted as a golden result. The X9-10
W10 augmentation subsequently passed targeted `--record` and fresh no-record
execution. The X9-02 W02 augmentation then passed the same targeted record and
fresh no-record sequence. The X9-06 pre-final W06 augmentation subsequently
passed a fresh targeted no-record run. Its diagnostic RED was an MTR accounting
error, not a production failure: all three survivor record objects produced
proof with zero object-prewarm misses, while the receiver `failed_tokens` gauge
correctly contained both the source-excluded ABORT and the injected
post-COMMIT selection failure. The paired X9-06 W16 increment then passed a
fresh targeted no-record run in 38.142 seconds: both epochs produced an exact
1 READY/3 failed partition, the failed records remained receiver-bound across
source restart while their staging/debt was cleared, and the READY sibling
remained saved/prepared. This completes the targeted W06/W16 runtime binding,
but formal X9-06 closure and any suite-wide passing claim still wait for the
complete fifty-six-file gate. The X9-07 W07 increment then passed a fresh targeted
no-record run in 12.399 seconds. At the pre-CAS boundary its source metadata
contained exactly two detached tokens and one live savepoint, while the
receiver had admitted and sealed real payload without a terminal CAS. RESET
produced one NOT_COMMITTED terminal CAS winner plus one exact idempotent
ABANDON duplicate and restored both original owners; the W07 owner committed
only its S1 and post-S2 effects, every rolled-back S2 update/delete/insert
remained absent, and the exact result survived source restart. The targeted
evidence does not claim physical promotion/adopt coverage, and formal X9-07
closure still waits for the complete fifty-six-file gate. The X9-09 W09
increment then passed a fresh targeted no-record run in 12.239 seconds. Its
first production RED left one 131072-byte image and one 65702-byte no-redo undo
sidecar after strict standby semantics rejected the mixed engine shape. The
narrow cleanup change treats a nonempty temporary-table manifest as token
artifact ownership even before the main snapshot exists, preserves the sealed
sidecars through generic removal, and lets metadata-aware cleanup release its
reservations before unlink. The original owner then continued both transaction
halves and rolled them back exactly; no receiver terminal/publication state or
source ownership ambiguity survived, and independent native commits survived
both restarts. This is rejection/cleanup evidence, not portable temporary-table
transfer support; formal X9-09 closure and any suite-wide passing claim still
wait for the complete fifty-six-file gate. The X9-11 W11/W12 increment then
passed a fresh targeted no-record run in 12.406 seconds. The real receiver
admitted and sealed all token payloads before the pre-CAS RESET; cleanup
produced exactly one NOT_COMMITTED CAS, zero conflicts, and one tombstone, with
no READY publication or retained receiver
resource. The restored original source THD preserved exact full-scan,
partition-pruned, secondary-index, collation, binary-key, NULL-index, and
numeric-boundary state, continued both tables, and committed once; independent
source and receiver commits survived both restarts. No production defect was
observed, so this increment changes only the MTR triplet and this ledger. The
X9-11 triplet remains part of the current expansion; formal X9-11 closure and
any suite-wide passing claim wait for the complete fifty-six-file gate. The
X9-12 W13/W14/W15 increment then passed a fresh targeted no-record run in
13.356 seconds. At the pre-CAS boundary, four real receiver token reservations
matched exactly three session-off/no-cache owners, one logged-cache AUTOMATIC
owner, three RC transactions, and one RR transaction with a nonempty ReadView.
RESET restored all four original source THDs, generated one NOT_COMMITTED CAS
with no conflict, and left no receiver publication or resource. The RC
reader changed only after its legal source commit, the RR owner retained its
old control version through RESET, the session-off comparison allocated no
GTID, and the logged owner alone committed source UUID GNO 1; exact data and
receiver-native effects survived both restarts. No production defect was
observed, so this increment changes only the MTR triplet and this ledger. The
suite now contains fifty-three matched nonempty `.test`/`.result`/`.cnf`
triplets; formal X9-12 closure and any suite-wide passing claim wait for the
complete fifty-six-file gate. The existing X7-01/X10-01/X10-09 augmentation
then exposed and closed one narrower production lifecycle gap. Before the fix,
RESET after an authenticated `OPEN_EPOCH` ACK but before the first
sequence-tracked frame restored the source and its blocked waiter correctly,
but source cleanup returned `UNSUPPORTED`; the receiver retained an unowned
OPEN-only map entry with no terminal CAS or tombstone, so only process restart
removed it. Three new GUnit contracts first failed 0/3 at the codec,
zero-declaration source cleanup, and no-record receiver ABANDON boundaries; the
two-mysqld MTR independently failed because both expected terminal counters
remained zero. The narrow fix reuses the existing process-bound
`ABANDON_EPOCH_IF_NOT_COMMITTED` path with the unique open-only shape
`sequence=1, token=0`, converts the context into one retry-safe bounded
tombstone, and leaves all token/payload/READY/debt surfaces empty. The fresh
targeted MTR passed with its shutdown report in 12.277 seconds, the three RED
GUnits passed 3/3, the ten-test codec/source/receiver adjacent GUnit selection
passed 10/10, and six adjacent standby-transfer scenarios plus shutdown report
passed 7/7. This augment-existing slice does not increase the fifty-three-file
count, does not claim physical adoption, and does not replace the pending full
suite gate.

The existing pre-CAS RESET case was then augmented with the runnable T1 half
of X10-02. A third source owner performs deterministic W03 cross-index DML
under RR, locks a 21-row nonunique-index range, and blocks a real adjacent
INSERT in the internal gap before DRAIN. The first no-record execution reached
the end with every new lock, ownership, index, and restart assertion true; its
only failure was the expected old-golden mismatch. The targeted record run
also completed every query, but the harness reported `errno: 1` while copying
post-run file metadata; the written result was byte-identical to the complete
run log with SHA256
`b2ffb75c543fdcb565ca6f3f765ecef7243c171a0f7637b748551e10839d0c94`.
A fresh no-record run then passed the scenario and shutdown report in 42.728
seconds. RESET preserved the original next-key owner and kept the waiter in
`data_lock_waits`; only that owner's explicit rollback released one committed
waiter insert. No production defect was observed, so this increment changes
only the existing MTR test/result and this ledger, keeps the suite at
fifty-three triplets, and does not replace the pending full-suite gate.

X10-03 was first placed at the same pre-CAS hook, where its adjacent source
INSERT correctly received ER 4020. Source audit showed that this hook executes
after `WARMCOPY_CLOSING` publishes the intentional all-command gate, so that
RED was a test synchronization error rather than an RC or transfer defect. The
increment was removed from the pre-CAS RESET case and moved to the existing
X12-06 `preserve_trx_warmcopy_after_phase1_open` window, the last phase where
new ordinary source commands are explicitly allowed. Its first complete run
detached both RR and RC targets and proved every source/index/restart assertion,
but the RC token reached `PREWARM_DEADLINE` because the receiver lacked its T1
dictionary identity. Replaying the empty source DDL order on the receiver and
checking the exact table/index/space fingerprint removed that test-fixture
gap: both tokens classified READY and the shadow remained empty before and
after restart. The record run completed all queries; only the harness metadata
copy reported `errno: 1`, while the written golden was byte-identical to the
complete run log with SHA256
`e97b2c03fa07a52f6f43bb37d6a9100d1829862bb5272f9cc26d4c4ca6f726ff`.
The fresh no-record run then passed the scenario and shutdown report in 12.260
seconds. This increment changes no production code and keeps the suite at
fifty-three triplets; the current full-suite gate remains pending.

X10-04 adds the fifty-fourth triplet as a distinct DDL/MDL failure lifecycle.
A W02 owner builds one exact FK-bound order with two surviving allocated
items, two inventory deltas, and a derived total while holding
transaction-duration MDL. After a real receiver OPEN/DECLARE admission, a
source `ALTER TABLE` requests EXCLUSIVE MDL and is proven PENDING behind the
owner. The first fixture RED was a receiver-native unique-index update collision
and was corrected without touching production code. The next RED returned
ER 4013 instead of the initially assumed closing exclusion; source logging
proved the intentional fail-closed branch with `transaction_count=2`,
`target_count=1`, and `has_unsupported_transaction=1`: the waiting DDL itself
owns an unsupported active transaction, so the whole epoch is rejected before
detach or terminal publication. The final test therefore requires the same
owner to remain live, the ALTER to remain blocked until owner rollback, and the
ALTER to publish exactly one durable column afterward. Receiver accounting
returns inflight bytes, staging, prepared state, workers, and queues to
baseline while retaining exactly two process-local ABORTED diagnostics until
receiver restart; both empty T1 shadows and unrelated native data remain
isolated. The record-path business run completed every query, but the harness
again reported post-run copy `errno: 1`; the written 12,410-byte golden was
byte-identical to the complete log with SHA256
`592a147d7872101fea642235996e7181c0205fb25cbe76fef6ca50aabf07280d`.
A fresh no-record run passed the scenario in 13.414 seconds plus
`shutdown_report` (2/2). No production file changed for X10-04. At that
checkpoint the suite contained fifty-four matched nonempty
`.test`/`.result`/`.cnf` triplets; formal X10-04 closure and any suite-wide
passing claim still require the pending fifty-six-file full gate.

X10-08 adds the fifty-fifth triplet as a real W16 overlapping-lock lifecycle.
The W01 owner holds one shared record while W05 completes a deterministic
64-row insert/update/delete model and then blocks on that record; independent
W07 savepoint and logged 512 KiB W08B owners complete the four-token epoch.
Phase1 selects four candidates, the fixed five-second closing deadline removes
only W05, and exactly three survivors reach the receiver pre-CAS boundary.
RESET restores those three owners and cleanly abandons receiver state without
releasing the wait edge. Only the restored W01 owner's explicit rollback lets
W05 execute and commit once; W07 commits while W01/W08B roll back, and every
exact logical/index result plus the receiver-native commit survives restart.
An early RED counted a provisional HA-control token because that control
session was itself waiting on DEBUG_SYNC during the Phase1 scan. Waiting first
on the three receiver leases and only then consuming the source signal removed
that test-induced declaration; the remaining failed token is exactly the W05
owner with reason `closing_command_timeout`. No production change was needed.
The record-path business run completed every query; only the known final copy
step reported `errno: 1`, while the written 24,362-byte golden was byte-identical
to the complete log with SHA256
`73f0b6bd07b95e92889447375a1e8c8019dc560cbfb69aa471ced7780dbfaa8a`.
A fresh no-record run passed in 13.297 seconds plus `shutdown_report` (2/2).
The suite now contains fifty-five matched nonempty triplets; formal X10-08
closure and any suite-wide passing claim still require the pending
fifty-six-file full gate.

The existing X9-03 triplet was then augmented with the runnable T1 portion of
X3-01.  Its W03 owner now stops after the original multi-chunk object payload
has been applied and replayed as `ALREADY_APPLIED`, then rewrites 512 indexed
rows, including 768 KiB of payload and every corresponding unique/secondary
key, while the independent outsider still sees the exact committed baseline.
A target-local DEBUG_SYNC signal can fire only when the quiesced Phase2 path
has queued that owner's final high-watermark descriptor.  The fresh run hit
that signal, and the source log independently recorded one 1,611,338-byte
final delta with `async_tokens=1`, `sync_fallback_tokens=0`, and
`pending_rejects=0`.  DRAIN, terminal retry, receiver cleanup, and both restart
checks then completed; all late keys disappeared with the rolled-back owner.
The first RED was only a fixture-privilege error when the restricted owner
could not register DEBUG_SYNC, so the case now grants the case-owned account
`SESSION_VARIABLES_ADMIN` until the shared teardown drops it.  No production
defect or production-code change was required.  A fresh no-record run passed
in 25.840 seconds plus `shutdown_report` (2/2); the 26,873-byte golden has
SHA256 `93ed8fd92e4a53cdb58b3f4434fb94b265179e76a776be1898fb7af510050078`.
This augment-existing slice keeps the suite at fifty-five triplets; formal
X3-01 closure and any suite-wide passing claim still require the pending
fifty-six-file full gate.

X3-10 adds the fifty-sixth triplet as a real final-fact/prewarm ordering race.
The receiver first consumes the W01 owner's Phase1 payload while its prewarm
worker is paused and source COMMIT is held before terminal ownership CAS. Once
prewarm is released, the prepared token must remain registered in
`PREWARMED_PENDING_FINAL_FACT`: accepted epoch, classification, READY, and
final-fact bind counters all remain unchanged. Releasing source COMMIT then
advances terminal CAS, tombstone, final-fact bind, classification, READY, and
prepared save exactly once; every queue, worker, staging, and cleanup-debt gauge
returns to baseline. The receiver shadow is identity-matched but data-empty,
and independent source/receiver-native commits survive their respective
restarts.

The first scheduling RED was caused by the HA control connection itself waiting
on DEBUG_SYNC while the Phase1 scanner was selecting non-idle sessions, which
temporarily declared an extra token and later pruned it. Waiting for real
receiver payload admission before that control session consumes the sticky
source signal removes the fixture-created candidate; source logging then proves
one early and one final survivor. No production code was changed. The record
business flow and both restarts completed, but MTR's final metadata copy exited
with `errno: 1`; the resulting 12,913-byte golden is byte-identical to the
complete log with SHA256
`5f10378c9e8d09b7c4081b0714757764c5e73ddbb48d98f719896c069a2076e6`, so that
record command is not counted as passing. A fresh no-record run passed the
scenario in 12.352 seconds plus `shutdown_report` (2/2). The suite now contains
fifty-six matched nonempty triplets, and X3-10 is included in the complete
passing gate below.

Current complete Debug gate after the X3-10 expansion:

```text
build-debug/mysql-test/mysql-test-run.pl \
  --suite=preserve_trx_transfer_stby \
  --parallel=8 --force --retry=0 \
  --vardir=/private/tmp/prt-full-20260815-transfer-green
```

The first complete run exposed only MTR asset drift: four existing
terminal-CAS goldens had not followed two intentional line breaks in their
shared include, and the receiver-partial-object crash case allowed only
`ACK_UNCERTAIN(9)` although the same expected dead-receiver abort can return
`IO_ERROR(3)`. The four goldens were synchronized and that one suppression was
narrowed to exactly `(3|9)`; no production file changed. A fresh serial gate of
those five business cases plus `shutdown_report` passed 6/6. The complete
fresh rerun then passed all fifty-six business scenarios plus
`shutdown_report`: 57/57, zero skipped, zero failed, no retries, runner exit 0.
This current result supersedes the earlier pending-gate checkpoint clauses in
the chronological notes above; catalog capability blockers remain separate
from this runnable MTR gate.
