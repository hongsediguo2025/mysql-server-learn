SELECT COUNT(*)=32 AS thirty_two_accounts,
       SUM(balance)=32001 AS committed_background_balance,
       SUM(version)=1 AS one_committed_background_version,
       MAX(CASE WHEN id=32 THEN balance END)=1001 AS background_update_visible,
       MAX(CASE WHEN id=3 THEN version END)=0 AS owner_updates_hidden
  FROM xfer2_w01_accounts;
SELECT COUNT(*)=0 AS no_committed_audit FROM xfer2_w01_audit;
