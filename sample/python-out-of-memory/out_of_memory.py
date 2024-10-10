from __future__ import annotations


class OutOfMemoryModel:
    def __init__(self):
        self._large_memory_allocation = [0] * (10**12)

    def perform(self) -> str:
        # If the init doesn't kill it, perform should
        self._large_memory_allocation = [0] * (10**12)
        return 'surprisingly, not dead'
