#!/usr/bin/env python3
"""Run the original full transfer gate with continuous tiered source SQL."""

import sys

from preserve_trx_full_pressure_runner import main


if __name__ == "__main__":
    sys.exit(
        main(sys.argv[1:], forced_evidence="continuous-tiered-transfer")
    )
