import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bench import env, lane


class WarmConditionTests(unittest.TestCase):
    def test_warm_checks_all_files_after_reading_them(self):
        events = []
        class Reader(io.BytesIO):
            def __init__(self, name):
                super().__init__(b'data')
                self.name = name
            def readinto(self, buf):
                events.append(('read', self.name))
                return super().readinto(buf)
        with patch('builtins.open', side_effect=lambda name, *a, **k: Reader(name)), \
             patch.object(env, 'residency', side_effect=lambda name: (events.append(('check', name)) or (1, 1))):
            self.assertTrue(env.warm(['a', 'b'])[0])
        self.assertLess(max(i for i, e in enumerate(events) if e[0] == 'read'),
                        min(i for i, e in enumerate(events) if e[0] == 'check'))

    def test_smoke_suppresses_only_busy(self):
        runner = object.__new__(lane.Lane)
        runner.lane, runner.smoke, runner.timing = 'hot', True, []
        with patch.object(env, 'warm', return_value=(False, {})), \
             patch.object(env, 'idle', return_value={'quiet': False, 'busy_cores': 2}), \
             patch('pathlib.Path.read_text', return_value='MemAvailable: 0 kB\n'):
            reasons, _ = runner.condition({'inputs': []})
        self.assertEqual(reasons, ['inputs-unverified', 'memory-before'])

    def test_inherited_lock_does_not_release_parent_lock(self):
        import fcntl
        with tempfile.TemporaryDirectory() as tmp, patch.object(env, 'LOCK', Path(tmp) / 'lock'):
            with env.timing_lock() as fd, patch.dict(os.environ, {'ZPQ_BENCH_LOCK_FD': str(fd)}):
                with env.timing_lock():
                    pass
                with env.LOCK.open('a') as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
