# Morsel Architecture Proof of Concept
#
# This module implements a Python prototype of the morsel-based parallel
# S3 writing architecture for zpq. The goal is to validate the state machine
# and coordination logic before implementing in Zig.
#
# Components:
#   coordinator.py   - MorselCoordinator state machine
#   worker.py        - MorselWorker (mock encoding)
#   pipeline.py      - Pipeline integration
#   parquet_footer.py - Footer building utilities

__version__ = "0.1.0"
