# Preserve/Resume 8.0.22 Review Checklist

This checklist is updated after every batch in the 8.0.22 port.

## Day 1 Checklist

- [x] Created branch `codex/preserve-resume-8.0.22-port`.
- [x] Verified base is `mysql-8.0.22`.
- [x] Wrote documentation only.
- [x] Did not migrate code.
- [x] Did not migrate tests.

Evidence:

```text
branch: codex/preserve-resume-8.0.22-port
HEAD: ee4455a33b10f1b1886044322e4893f587b319ed
base: mysql-8.0.22
changed files: design/*.md only
source/test migration files: none
```

## Per-Batch Checklist Template

For each batch, copy this section and fill it in before committing the batch.

### Batch N: <name>

- [ ] Test inventory updated.
- [ ] Code inventory updated.
- [ ] Commit manifest rows updated.
- [ ] Test manifest rows updated.
- [ ] Round A feature-off / unsupported targeted MTR passed in debug.
- [ ] Round A feature-off / unsupported targeted MTR passed in release.
- [ ] Round B RED was observed and recorded, or `N/A with reason` for Batch 0.
- [ ] Round B GREEN passed in debug, or `N/A with reason` for Batch 0.
- [ ] Round B GREEN passed in release, or `N/A with reason` for Batch 0.
- [ ] Touched explicit conflict files reviewed.
- [ ] Touched changed-both files reviewed.
- [ ] Expected-but-untouched conflict/overlap files justified.
- [ ] `git diff --check` passed.
- [ ] `git status --short` reviewed.
- [ ] `git branch --show-current` verified target branch.
- [ ] `git diff --name-only --cached` reviewed for batch scope.
- [ ] 3 independent sub-agent reviews completed.
- [ ] All Blocker/Major review findings fixed or rejected with evidence.
- [ ] Batch commit created.

Review findings summary:

```text
Commands/results:
Round A:
Round B:
Conflict/overlap disposition:
Reviewer A:
Reviewer B:
Reviewer C:
Resolution:
Commit:
```

## Final Review Checklist

- [ ] 5 independent full-review sub agents completed.
- [ ] All 123 source commits represented or explicitly superseded.
- [ ] All 239 MTR `.test` files migrated, adapted, or explicitly deferred.
- [ ] All 240 changed `.result` files migrated, adapted, or explicitly deferred.
- [ ] All 4 preserve gunit files migrated.
- [ ] Python E2E and benchmark scripts migrated and run.
- [ ] Python unit tests migrated and run.
- [ ] 30 explicit conflict files reviewed.
- [ ] 66 changed-both files reviewed.
- [ ] Feature-off behavior remains equivalent to original 8.0.22.
- [ ] Warm-copy behavior is isolated and verified.
- [ ] User temporary table behavior is isolated and verified.
- [ ] No `.result` update masks a product bug.
- [ ] Final debug/release build gates passed.
- [ ] Final debug/release gunit gates passed, including `trx0preserve-t`.
- [ ] Final debug/release preserve_trx MTR gates passed with log-bin and no-bin.
- [ ] Final debug/release preserve_trx big-test gates passed.
- [ ] Final perfschema `dml_handler` targeted gates passed.
- [ ] Final Python E2E, benchmark, and Python unit-test gates passed.
- [ ] Full MySQL MTR or CI/release farm gate passed.
- [ ] Any excluded baseline failures reproduced on untouched `mysql-8.0.22`.
