#!/usr/bin/env python3

import sys

from preserve_trx_full_pressure_runner import main


if __name__ == "__main__":
    raise SystemExit(
        main(
            sys.argv[1:],
            forced_evidence="dependency-continuous-large-tx-transfer",
            forced_large_tx_no_commit=True,
        )
    )
