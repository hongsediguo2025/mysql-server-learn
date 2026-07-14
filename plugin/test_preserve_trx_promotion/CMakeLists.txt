# Copyright (c) 2026, Oracle and/or its affiliates.

MYSQL_ADD_PLUGIN(test_preserve_trx_promotion
  test_preserve_trx_promotion.cc
  MODULE_ONLY
  MODULE_OUTPUT_NAME "libtest_preserve_trx_promotion"
  TEST_ONLY
  )
