"""Verification for the external (spill) execution mode.

Stdlib-only (the project's third-party stack is unavailable in this sandbox):
run with  python3 -I -m tests.spill_verify  from the backend directory.

Covers the required scenarios - hot-key multiplicity, many groups, empty
input, disk failure - plus memory/spill semantic parity, the 500-row memory
ceiling, atomic publish/cleanup, the 200k join ceiling and full export.
"""
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# isolated temp SPILL_ROOT before importing engine modules
TMP = tempfile.mkdtemp(prefix='spill-verify-')
os.environ['SPILL_ROOT'] = os.path.join(TMP, 'artifacts')
os.environ.setdefault('JWT_SECRET', 'test-secret')

from src.services import operators, spill  # noqa: E402

FULL_STEPS = [
    {'op': 'scan', 'source': 'readings'},
    {'op': 'filter', 'field': 'value', 'compare': 'gte', 'value': 0},
    {'op': 'hash_join', 'right': 'sensors', 'left_key': 'sensor_id',
     'right_key': 'sensor_id'},
    {'op': 'group', 'keys': ['zone'], 'measure': 'value', 'aggregate': 'avg'},
]


def sensor(sid, zone=None, device='pump'):
    return {'sensor_id': sid, 'zone': zone or f'z{sid}', 'device_type': device}


def reading(sid, value, at='2026-11-17T09:00:00'):
    return {'sensor_id': sid, 'value': value, 'recorded_at': at}


def dataset(sensors, readings):
    return {'sensors': sensors, 'readings': readings}


class SpillTests(unittest.TestCase):
    # class-level counter keeps run ids unique across every test method so a
    # committed artifact of one test can never shadow a failed run of another
    _seq = 0

    def setUp(self):
        pass

    @classmethod
    def next_id(cls):
        cls._seq += 1
        return 900000 + cls._seq

    def run_spill(self, ds, steps=FULL_STEPS):
        self.last_id = self.next_id()
        return spill.execute_spill(ds, steps, run_id=self.last_id)

    def last_output(self):
        return list(spill.load_output(self.last_id))

    def assertSameRows(self, a, b):
        self.assertEqual(json.loads(json.dumps(a, sort_keys=True)),
                         json.loads(json.dumps(b, sort_keys=True)))

    # 1. semantic parity with the original in-memory engine ----------------
    def test_parity_small_pipeline(self):
        sensors = [sensor(i, f'zone{i % 3}') for i in range(1, 9)]
        readings = [reading(i % 8 + 1, round((i * 7) % 100 + 0.5, 3))
                    for i in range(120)]
        ds = dataset(sensors, readings)
        mem = operators.execute(ds, FULL_STEPS)
        sp = self.run_spill(ds)
        self.assertEqual(sp['mode'], 'spill')
        # per-operator row counts, final rows, ordering and checksum agree
        self.assertEqual([(o['op'], o['input_rows'], o['output_rows'])
                          for o in mem['operators']],
                         [(o['op'], o['input_rows'], o['output_rows'])
                          for o in sp['operators']])
        self.assertEqual(mem['output_rows'], sp['output_rows'])
        self.assertEqual(mem['checksum'], sp['checksum'])
        self.assertSameRows(mem['output'], self.last_output())

    def test_parity_count_sum_avg_and_two_keys(self):
        sensors = [sensor(i, f'z{i % 2}', ['pump', 'motor'][i % 2])
                   for i in range(1, 7)]
        readings = [reading(i % 6 + 1, float((i * 13) % 50) + 0.25)
                    for i in range(90)]
        ds = dataset(sensors, readings)
        for agg in ('count', 'sum', 'avg'):
            steps = [FULL_STEPS[0], FULL_STEPS[2],
                     {'op': 'group', 'keys': ['zone', 'device_type'],
                      'measure': 'value', 'aggregate': agg}]
            mem = operators.execute(ds, steps)
            sp = self.run_spill(ds, steps)
            self.assertEqual(mem['checksum'], sp['checksum'], agg)
            self.assertEqual(mem['output'], self.last_output(), agg)

    def test_parity_filter_after_join(self):
        # join then continue filtering, as required for the 200k scenario
        sensors = [sensor(i, f'z{i % 4}') for i in range(1, 6)]
        readings = [reading(i % 5 + 1, float(i % 80)) for i in range(80)]
        ds = dataset(sensors, readings)
        steps = [FULL_STEPS[0], FULL_STEPS[2],
                 {'op': 'filter', 'field': 'zone', 'compare': 'eq', 'value': 'z1'},
                 FULL_STEPS[3]]
        mem = operators.execute(ds, steps)
        sp = self.run_spill(ds, steps)
        self.assertEqual(mem['checksum'], sp['checksum'])
        self.assertEqual(mem['output'], self.last_output())

    # 2. hot key: duplicate right keys retain multiplicity -----------------
    def test_hot_key_multiplicity(self):
        sensors = [sensor(1, 'north'), sensor(1, 'audit'), sensor(2, 'south')]
        readings = [reading(1, 10.0), reading(1, 20.0), reading(2, 30.0)]
        ds = dataset(sensors, readings)
        sp = self.run_spill(ds, FULL_STEPS[:3])
        rows = self.last_output()
        # two readings on key 1 each match two sensors -> 4 rows, key 2 -> 1
        self.assertEqual(len(rows), 5)
        self.assertEqual(sum(1 for r in rows if r['sensor_id'] == 1), 4)
        mem = operators.execute(ds, FULL_STEPS[:3])
        self.assertEqual(mem['checksum'], sp['checksum'])
        self.assertLessEqual(sp['peak_buffered_rows'], spill.BATCH)

    # 3. many groups stay within the 500-row ceiling -----------------------
    def test_many_groups_memory_ceiling(self):
        # 100 sensors, each in its own zone -> 100 distinct groups; the
        # 2000 readings use the existing input cap
        sensors = [sensor(i, zone=f'zone{i}') for i in range(1, 101)]
        readings = [reading(i % 100 + 1, float(i % 70) + 0.1)
                    for i in range(2000)]
        ds = dataset(sensors, readings)
        sp = self.run_spill(ds)
        self.assertEqual(sp['operators'][-1]['detail']['groups'], 100)
        self.assertLessEqual(sp['peak_buffered_rows'], spill.BATCH)
        mem = operators.execute(ds, FULL_STEPS)
        self.assertEqual(mem['checksum'], sp['checksum'])
        # more than one shard proves intermediate data actually spilled
        self.assertGreater(len(sp['artifact']['files']), 1)

    # 4. 200k joined rows, then filter and group still work ----------------
    def test_200k_join_then_filter_and_group(self):
        # two duplicate right rows on the same hot key double every probe:
        # 100k readings x 2 sensors -> exactly 200000 joined rows
        sensors = [sensor(1, 'north'), sensor(1, 'south')]
        readings = [reading(1, float(i % 60)) for i in range(100000)]
        # readings are capped at 2000 by input validation, so this engine
        # test injects a pre-validated large snapshot directly
        sp = spill.execute_spill(dataset(sensors, readings), [
            FULL_STEPS[0], FULL_STEPS[2],
            {'op': 'filter', 'field': 'zone', 'compare': 'eq', 'value': 'north'},
            {'op': 'group', 'keys': ['zone'], 'measure': 'value',
             'aggregate': 'sum'},
        ], run_id=self.next_id())
        self.assertEqual(sp['operators'][1]['output_rows'], 200000)
        self.assertLessEqual(sp['peak_buffered_rows'], spill.BATCH)
        out = list(spill.load_output(900000 + type(self)._seq))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]['zone'], 'north')
        # filter keeps one of the two duplicate-zone copies per reading, so
        # the grouped sum equals the sum of every reading's value
        expected = sum(float(i % 60) for i in range(100000))
        self.assertEqual(out[0]['sum'], round(expected, 6))
        # many shards on disk, none of the 200k rows held at once
        self.assertGreater(len(sp['artifact']['files']), 40)

    def test_join_ceiling_rejected_and_cleaned(self):
        # 2000 readings x 101 duplicate sensors > 200k joined rows
        sensors = [sensor(1) for _ in range(101)]
        readings = [reading(1, 1.0) for _ in range(2000)]
        run_id = self.next_id()
        with self.assertRaises(ValueError):
            spill.execute_spill(dataset(sensors, readings), FULL_STEPS[:3],
                                run_id=run_id)
        # failure leaves no final directory and no temp build dir
        self.assertFalse(os.path.exists(os.path.join(
            spill.SPILL_ROOT, f'trace-{run_id}')))
        self.assertEqual([n for n in os.listdir(spill.SPILL_ROOT)
                          if n.startswith(f'.trace-{run_id}')], [])

    # 5. empty input --------------------------------------------------------
    def test_empty_input(self):
        sp = self.run_spill(dataset([], []))
        self.assertEqual(sp['output_rows'], 0)
        self.assertEqual(sp['operators'][0]['detail']['scanned_rows'], 0)
        self.assertEqual(self.last_output(), [])
        mem = operators.execute(dataset([], []), FULL_STEPS)
        self.assertEqual(mem['checksum'], sp['checksum'])
        # an empty run still publishes a manifest atomically
        self.assertTrue(os.path.exists(sp['artifact']['manifest']))

    # 6. disk failure mid-write cleans up and keeps failure ----------------
    def test_disk_failure_cleans_temp_artifacts(self):
        run_id = self.next_id()
        run = spill.SpillRun(run_id)
        real_write = run._path
        # simulate a disk fault on the second data file write
        calls = {'n': 0}

        def boom(name):
            path = real_write(name)
            if name.endswith('.jsonl'):
                calls['n'] += 1
                if calls['n'] == 2:
                    raise OSError('simulated disk failure')
            return path

        run._path = boom
        w = run.writer()
        with self.assertRaises(OSError):
            for i in range(spill.BATCH + 5):
                w.add(reading(1, float(i)))
        run.cleanup()
        self.assertFalse(os.path.exists(run.dir))
        self.assertFalse(os.path.exists(os.path.join(
            spill.SPILL_ROOT, f'trace-{run_id}')))

    def test_disk_failure_during_full_execution(self):
        run_id = self.next_id()
        import builtins
        original_open = builtins.open
        # the first tmp data file faults once its first BATCH lines are
        # written, emulating a disk fault while intermediate results spill
        def flaky_open(path, *a, **kw):
            fh = original_open(path, *a, **kw)
            if isinstance(path, str) and path.endswith('.jsonl.tmp'):
                real_write = fh.write

                def guarded_write(text):
                    if text == '\n' and fh.line_count + 1 >= spill.BATCH:
                        raise OSError('simulated write fault')
                    n = real_write(text)
                    if text == '\n':
                        fh.line_count += 1
                    return n

                fh.line_count = 0
                fh.write = guarded_write
            return fh

        builtins.open = flaky_open
        try:
            sensors = [sensor(1)]
            readings = [reading(1, 1.0) for _ in range(1200)]
            with self.assertRaises(OSError):
                spill.execute_spill(dataset(sensors, readings), FULL_STEPS[:3],
                                    run_id=run_id)
        finally:
            builtins.open = original_open
        self.assertFalse(os.path.exists(os.path.join(
            spill.SPILL_ROOT, f'trace-{run_id}')))
        self.assertEqual([n for n in os.listdir(spill.SPILL_ROOT)
                          if f'trace-{run_id}' in n], [])

    # 7. atomic publish: manifest last, dir switched once ------------------
    def test_manifest_lists_every_shard_and_export_roundtrip(self):
        sensors = [sensor(i, f'z{i}') for i in range(1, 11)]
        readings = [reading(i % 10 + 1, float(i)) for i in range(1500)]
        sp = self.run_spill(dataset(sensors, readings))
        base = sp['artifact']['dir']
        with open(sp['artifact']['manifest'], encoding='utf-8') as f:
            manifest = json.load(f)
        names = {e['name'] for e in manifest['files']}
        on_disk = {n for n in os.listdir(base) if n.endswith('.jsonl')}
        self.assertEqual(names, on_disk)
        # manifest marks which shards form the final output table
        row_index = {e['name']: e['rows'] for e in manifest['files']}
        self.assertTrue(set(manifest['output_files']).issubset(names))
        self.assertEqual(sum(row_index[n] for n in manifest['output_files']),
                         sp['output_rows'])
        # intermediate shards are retained as evidence but excluded from export
        self.assertGreater(len(names), len(manifest['output_files']))
        # full export parses as one JSON document with the complete output
        item = {'id': self.last_id, 'status': 'completed',
                'mode': 'spill', 'result': sp, 'snapshot': {}}
        doc = json.loads(''.join(spill.build_export(item)))
        self.assertEqual(len(doc['result']['output']), sp['output_rows'])
        self.assertNotIn('output_preview', doc['result'])
        self.assertEqual(doc['result']['checksum'], sp['checksum'])

    def test_memory_trace_export_unchanged(self):
        item = {'id': 1, 'status': 'completed', 'mode': 'memory',
                'result': {'output': [{'a': 1}], 'checksum': 'x'}}
        doc = json.loads(''.join(spill.build_export(item)))
        self.assertEqual(doc, item)

    def test_validate_still_rejects_bad_plans(self):
        with self.assertRaises(ValueError):
            operators.validate_steps([{'op': 'filter'}])
        with self.assertRaises(ValueError):
            spill.execute_spill(dataset([], []),
                                [{'op': 'scan', 'source': 'sensors'}],
                                self.next_id())

    # 8. the 500-row ceiling is enforced, not just observed ----------------
    def test_guard_raises_above_batch(self):
        guard = spill.Guard()
        guard.hold(spill.BATCH)
        with self.assertRaises(RuntimeError):
            guard.hold(spill.BATCH + 1)
        # non-strict guard records the high-water mark without raising
        soft = spill.Guard(strict=False)
        soft.hold(spill.BATCH + 7)
        self.assertEqual(soft.peak, spill.BATCH + 7)

    def test_external_sort_empty_and_memory_bound(self):
        run = spill.SpillRun(self.next_id())
        try:
            empty = run.spill_rows(iter([]))
            self.assertEqual(run.external_sort(empty, spill._row_dumps)['rows'], 0)
            # 1300 rows span three BATCH files and force a multi-way merge,
            # never exceeding the BATCH buffered-row ceiling
            table = run.spill_rows(
                reading(i, float(i)) for i in range(1300))
            ordered = run.external_sort(
                table, lambda r: r['recorded_at'] + str(r['value']))
            self.assertEqual(ordered['rows'], 1300)
            self.assertLessEqual(run.guard.peak, spill.BATCH)
            run.commit(ordered)
        finally:
            run.cleanup()

    def test_200k_peak_is_honestly_below_batch(self):
        # explicit assertion surfaced from the main 200k scenario
        sensors = [sensor(1, 'north'), sensor(1, 'south')]
        readings = [reading(1, 1.0) for _ in range(2000)]
        sp = spill.execute_spill(dataset(sensors, readings), FULL_STEPS[:3],
                                 run_id=self.next_id())
        self.assertEqual(sp['output_rows'], 4000)
        self.assertLessEqual(sp['peak_buffered_rows'], spill.BATCH)
        self.assertEqual(sp['memory_batch'], spill.BATCH)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(TMP, ignore_errors=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
