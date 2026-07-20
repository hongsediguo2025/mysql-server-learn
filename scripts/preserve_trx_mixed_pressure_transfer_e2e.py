#!/usr/bin/env python3
"""Run the rich mixed-pressure source/receiver transfer E2E."""

import sys

from preserve_trx_full_pressure_runner import main


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], forced_evidence="mixed-transfer"))
