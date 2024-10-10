from __future__ import annotations

class CrashModel:
    def __init__(self):
        import time, os
        time.sleep(5)
        os._exit(1)

    def perform(self) -> float:
        return 43
