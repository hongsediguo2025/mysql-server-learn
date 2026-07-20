#!/usr/bin/env python3
"""Run the rich mixed-pressure local shutdown/startup Preserve/Resume E2E."""

import sys

from preserve_trx_full_pressure_runner import main


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], forced_evidence="mixed-shutdown-startup"))
