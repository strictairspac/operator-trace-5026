"""External (spill) execution mode.

Same bounded DSL and same teaching semantics as services.operators, but every
intermediate stage beyond the immutable input snapshot is streamed through
spill files under SPILL_ROOT. At most BATCH intermediate rows are buffered in
memory at any moment, so a single hot key or a very large number of groups is
bounded exactly like the normal path.

Spill layout (one run directory per trace), all writes go to *.tmp siblings
and are atomically renamed, the manifest is written last and the whole run
directory itself is switched in atomically:

  <SPILL_ROOT>/trace-<id>/
    stage-<n>-<k>.jsonl   one JSON object per compact line; regular stages stay
                          at BATCH rows per file, merge waves emit one file
    manifest.json         final completeness marker, per-file row counts and
                          the list of shards forming the final output table

A failed run removes its whole temporary directory; callers only ever see a
finished directory whose manifest lists every file, never a half-written one.
"""
import os
import json
import time
import hashlib
import heapq
import shutil
import tempfile
from decimal import Decimal
from src.config.settings import SPILL_ROOT

#: hard ceiling on rows buffered in memory at any one moment (intermediate
#: records only; the immutable input snapshot itself is not an intermediate)
BATCH = 500
#: spill mode keeps the existing input caps but raises the joined-row ceiling
MAX_SPILL_JOIN_ROWS = 200000
#: rows per sorted-run buffer; also the hard memory ceiling (BATCH). Merge
#: waves hold one heap head per input run, at most MERGE_FANIN heads at once
MERGE_FANIN = 10


def _row_dumps(row):
    # compact, deterministic; JSONL lines stay independent of checksum style
    return json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _group_key(row, keys):
    # json.dumps of the raw key tuple is a stable total-order surrogate,
    # equal keys serialize identically
    return json.dumps(tuple(row[k] for k in keys), sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'))


class Guard:
    """Tracks the high-water mark of intermediate rows held in memory."""

    def __init__(self, strict=True):
        self.peak = 0
        self.strict = strict

    def hold(self, n):
        self.peak = max(self.peak, n)
        if self.strict and n > BATCH:
            raise RuntimeError(f'外排缓冲区超过{BATCH}行上限（实际{n}行）')


class Checksum:
    """Streaming SHA-256 identical to operators.checksum over a full list."""

    def __init__(self):
        self._h = hashlib.sha256()
        self._first = True

    def add(self, row):
        if self._first:
            self._h.update(b'[')
            self._first = False
        else:
            self._h.update(b',')
        self._h.update(_row_dumps(row).encode('utf-8'))

    def hexdigest(self):
        if self._first:
            self._h.update(b'[')
        self._h.update(b']')
        return self._h.hexdigest()


class SpillRun:
    """All files belonging to one external execution, cleaned on failure."""

    def __init__(self, run_id, guard=None):
        self.run_id = run_id
        self.guard = guard or Guard()
        self.final_dir = os.path.join(SPILL_ROOT, f'trace-{run_id}')
        # build under a unique temporary sibling so the final directory only
        # appears atomically once the manifest is complete
        parent = os.path.dirname(self.final_dir) or '.'
        os.makedirs(parent, exist_ok=True)
        self.dir = tempfile.mkdtemp(prefix=f'.trace-{run_id}-', dir=parent)
        self.files = []
        # file names belonging to the final output table; every other shard is
        # an intermediate stage kept as evidence but not part of the export
        self.output_files = []
        self._label = 0
        self._committed = False

    # -- low level -------------------------------------------------------
    def _path(self, name):
        return os.path.join(self.dir, name)

    def next_label(self):
        """Unique file label across the whole run (no writer name collisions)."""
        self._label += 1
        return self._label

    def writer(self, flush_at=BATCH):
        return _BufferedWriter(self, self.next_label(), flush_at=flush_at)
    def reader(self, table):
        return _RowReader(self, table)

    def write_json_atomic(self, name, payload):
        tmp = self._path(name + '.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path(name))

    def commit(self, output_table):
        """Write the manifest last, then atomically publish the directory."""
        manifest = {
            'run_id': self.run_id,
            'files': self.files,
            'output_files': list(output_table['files']),
        }
        self.output_files = list(output_table['files'])
        self.write_json_atomic('manifest.json', manifest)
        if os.path.exists(self.final_dir):
            # leftovers of an earlier attempt are never partially visible:
            # replace only after the new directory is fully built
            shutil.rmtree(self.final_dir)
        os.replace(self.dir, self.final_dir)
        # post-commit reads (preview/export checks) resolve the published path
        self.dir = self.final_dir
        self._committed = True
        return self.final_dir

    def cleanup(self):
        if not self._committed and os.path.isdir(self.dir):
            shutil.rmtree(self.dir, ignore_errors=True)

    # -- operators -------------------------------------------------------
    def spill_rows(self, rows):
        """Persist any iterable of rows in BATCH-sized files."""
        w = self.writer()
        for row in rows:
            w.add(row)
        return w.close()

    def external_sort(self, table, keyfn):
        """Sort a spilled table by keyfn(row) with bounded-memory merges.

        Phase 1 reads BATCH rows at a time into one buffer, sorts it in memory
        and streams it out as a sorted run. Phase 2 merges up to MERGE_FANIN
        runs per wave into a single unrotated output file, so every pass
        reduces the file count and the loop always converges; the only rows in
        memory during a merge are the one heap head per input run.
        """
        # 1. sorted runs, each produced from at most BATCH buffered rows
        run_table = {'files': [], 'rows': 0}

        def drain_sorted(buf, w):
            # sort descending so pop() yields ascending order one row at a time
            buf.sort(key=keyfn, reverse=True)
            while buf:
                w.add(buf.pop())
                self.guard.hold(len(buf))

        for src in table['files']:
            buf = []
            with open(self._path(src), 'r', encoding='utf-8') as f:
                for line in f:
                    buf.append(json.loads(line))
                    run_table['rows'] += 1
                    self.guard.hold(len(buf))
                    if len(buf) == BATCH:
                        w = self.writer()
                        drain_sorted(buf, w)
                        run_table['files'].extend(w.close()['files'])
                        buf = []
            if buf:
                w = self.writer()
                drain_sorted(buf, w)
                run_table['files'].extend(w.close()['files'])
        # 2. multi-way merge waves; one head per input run is the only rows
        # held, and each wave emits exactly one output file
        while len(run_table['files']) > 1:
            next_files = []
            for i in range(0, len(run_table['files']), MERGE_FANIN):
                wave = run_table['files'][i:i + MERGE_FANIN]
                w = self.writer(flush_at=None)
                heads = []
                streams = [open(self._path(name), 'r', encoding='utf-8') for name in wave]
                try:
                    for idx, st in enumerate(streams):
                        line = st.readline()
                        if line:
                            row = json.loads(line)
                            heads.append((keyfn(row), idx, row))
                    self.guard.hold(len(heads))
                    heapq.heapify(heads)
                    while heads:
                        _, idx, row = heapq.heappop(heads)
                        w.add(row)
                        line = streams[idx].readline()
                        if line:
                            nxt = json.loads(line)
                            heapq.heappush(heads, (keyfn(nxt), idx, nxt))
                        self.guard.hold(len(heads))
                finally:
                    for st in streams:
                        st.close()
                next_files.extend(w.close()['files'])
            run_table = {'files': next_files, 'rows': run_table['rows']}
        return run_table

    def streaming_group(self, sorted_table, keys, agg):
        """Aggregate rows already sorted by group key, one group at a time."""
        w = self.writer()
        cur_key = object()
        cur_raw = None
        total = 0
        decimal_sum = Decimal('0')
        count = 0

        def flush_group():
            nonlocal total
            if cur_raw is None:
                return
            if agg == 'count':
                value = count
            elif agg == 'sum':
                value = round(float(decimal_sum), 6)
            else:
                value = round(float(decimal_sum / count), 6)
            out = dict(zip(keys, cur_raw))
            out[agg] = value
            w.add(out)
            total += 1

        with self.reader(sorted_table) as rows:
            for row in rows:
                raw = tuple(row[k] for k in keys)
                key = json.dumps(raw, sort_keys=True, ensure_ascii=False,
                                  separators=(',', ':'))
                if key != cur_key:
                    flush_group()
                    cur_key = key
                    cur_raw = raw
                    decimal_sum = Decimal('0')
                    count = 0
                count += 1
                decimal_sum += Decimal(str(row['value']))
                # only the current group's single accumulator is retained
                self.guard.hold(1)
        flush_group()
        table = w.close()
        # operators.execute emits group rows ordered by canonical full-row JSON
        return self.external_sort(table, _row_dumps), total


class _BufferedWriter:
    """Streams rows to atomic JSONL files without retaining them.

    flush_at caps the rows per file (BATCH for normal stages, which keeps
    every data shard bounded); flush_at=None writes one unrotated file, used
    by merge waves so each wave collapses its inputs into a single output and
    the merge pass always converges. Rows pass straight through, so this
    writer never holds an intermediate row in memory.
    """

    def __init__(self, run, label, flush_at=BATCH):
        self.run = run
        self.label = label
        self.flush_at = flush_at
        self._seq = 0
        self.rows_buffered = 0  # streaming writer: no row is ever retained
        self.files = []
        self.rows_written = 0
        self._file_rows = 0
        self._fh = None
        self._path = None

    def _open_file(self):
        name = f'stage-{self.label}-{self._seq}.jsonl'
        self._path = self.run._path(name)
        self._fh = open(self._path + '.tmp', 'w', encoding='utf-8')
        self._file_rows = 0
        return name

    def add(self, row):
        if self._fh is None:
            self._open_file()
        self._fh.write(_row_dumps(row))
        self._fh.write('\n')
        self.rows_written += 1
        self._file_rows += 1
        if self.flush_at is not None and self._file_rows >= self.flush_at:
            self._finish_file()

    def _finish_file(self):
        if not self._fh:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        name = os.path.basename(self._path)
        os.replace(self._path + '.tmp', self._path)
        self.run.files.append({'name': name, 'rows': self._file_rows})
        self.files.append(name)
        self._seq += 1
        self._fh = None

    def close(self):
        self._finish_file()
        return {'files': list(self.files), 'rows': self.rows_written}


class _RowReader:
    """Yields rows from a spilled table one at a time."""

    def __init__(self, run, table):
        self.run = run
        self.table = table

    def __enter__(self):
        return self._iter()

    def __exit__(self, *exc):
        return False

    def _iter(self):
        for name in self.table['files']:
            with open(self.run._path(name), 'r', encoding='utf-8') as f:
                for line in f:
                    yield json.loads(line)


def _checksum_table(run, table):
    h = Checksum()
    with run.reader(table) as rows:
        for row in rows:
            h.add(row)
    return h.hexdigest()


def _preview(run, table, n=5):
    out = []
    with run.reader(table) as rows:
        for row in rows:
            out.append(row)
            if len(out) >= n:
                break
    return out


def execute_spill(dataset, steps, run_id):
    """External-mode counterpart of operators.execute with equal semantics."""
    from src.services.operators import validate_steps
    validate_steps(steps)
    run = SpillRun(run_id)
    guard = run.guard
    total_started = time.perf_counter()
    visits = 0
    trace = []
    table = {'files': [], 'rows': 0}
    try:
        for step in steps:
            started = time.perf_counter()
            before = table['rows']
            detail = {}
            if step['op'] == 'scan':
                # the snapshot itself is the bounded input, not an intermediate
                def scanned():
                    for r in dataset['readings']:
                        yield dict(r)
                table = run.spill_rows(scanned())
                visits += table['rows']
                detail = {'source': 'readings', 'scanned_rows': table['rows']}
            elif step['op'] == 'filter':
                field, cmp, value = step['field'], step['compare'], step['value']

                def match(row):
                    v = row[field]
                    if cmp == 'eq':
                        return v == value
                    if not isinstance(v, (int, float)):
                        raise ValueError('范围比较遇到非数值')
                    return v >= value if cmp == 'gte' else v <= value

                w = run.writer()
                visits += before
                with run.reader(table) as rows:
                    for row in rows:
                        if match(row):
                            # streaming writer retains nothing; the single row
                            # in hand is the only intermediate record held
                            guard.hold(1)
                            w.add(row)
                table = w.close()
                detail = {'predicate': step, 'removed_rows': before - table['rows']}
            elif step['op'] == 'hash_join':
                # right side is capped at 100 sensors by the existing input limit
                buckets = {}
                for sensor in dataset['sensors']:
                    buckets.setdefault(sensor['sensor_id'], []).append(sensor)
                guard.hold(sum(len(v) for v in buckets.values()))
                probes = 0
                joined = 0
                w = run.writer()
                visits += before + len(dataset['sensors'])
                with run.reader(table) as rows:
                    for row in rows:
                        probes += 1
                        matches = buckets.get(row['sensor_id'], ())
                        # hot right keys retain their full multiplicity, but
                        # matches stream straight to disk, never accumulating
                        # as one giant in-memory list
                        for sensor in matches:
                            w.add({**row, **sensor})
                            joined += 1
                            guard.hold(len(buckets.get(row['sensor_id'], ())) + 1)
                            if joined > MAX_SPILL_JOIN_ROWS:
                                raise ValueError(
                                    f'外排连接结果超过{MAX_SPILL_JOIN_ROWS}行，请缩小输入；未保存部分结果')
                table = w.close()
                detail = {'build_rows': len(dataset['sensors']),
                          'hash_buckets': len(buckets), 'probe_rows': probes,
                          'semantics': 'inner join; duplicate right keys retain multiplicity'}
            elif step['op'] == 'group':
                keys = step['keys']
                agg = step['aggregate']
                visits += before
                # sort on the group key so one key's rows arrive consecutively
                key_sorted = run.external_sort(
                    table, lambda r, ks=keys: _group_key(r, ks))
                table, groups = run.streaming_group(key_sorted, keys, agg)
                detail = {'groups': groups, 'aggregate': agg,
                          'accumulator': 'decimal from source string; rounded 6 decimals'}
            trace.append({
                'op': step['op'], 'input_rows': before, 'output_rows': table['rows'],
                'duration_ms': round((time.perf_counter() - started) * 1000, 4),
                'detail': detail, 'checksum': _checksum_table(run, table),
                'preview': _preview(run, table),
            })
        final_checksum = _checksum_table(run, table)
        artifact_dir = run.commit(table)
    except Exception:
        # any failure (including disk errors mid-write) removes every partial
        # artifact; the trace stays 'failed' and no half result is exposed
        run.cleanup()
        raise
    return {
        'mode': 'spill',
        'operators': trace,
        'output_rows': table['rows'],
        'output_preview': _preview(run, table),
        'checksum': final_checksum,
        'row_visits': visits,
        'duration_ms': round((time.perf_counter() - total_started) * 1000, 4),
        'peak_buffered_rows': guard.peak,
        'memory_batch': BATCH,
        'limits': {'max_readings': 2000, 'max_sensors': 100,
                   'max_join_rows': MAX_SPILL_JOIN_ROWS, 'max_stages': 6},
        'artifact': {'dir': artifact_dir, 'files': run.files,
                     'manifest': os.path.join(artifact_dir, 'manifest.json')},
        'model': 'Python teaching external scan/filter/hash join/group engine; '
                 'PostgreSQL persists business metadata only',
    }


def load_output(run_id):
    """Stream the full final output of a committed spill run as row dicts."""
    base = os.path.join(SPILL_ROOT, f'trace-{run_id}')
    manifest_path = os.path.join(base, 'manifest.json')
    if not os.path.exists(manifest_path):
        raise FileNotFoundError('外排结果不完整或已被清理')
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    # only the shards marked as the final output table belong to the export;
    # intermediate stage shards are retained on disk as operator evidence
    for name in manifest['output_files']:
        path = os.path.join(base, name)
        if not os.path.exists(path):
            raise FileNotFoundError('外排结果缺少数据分片，无法导出')
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)


def build_export(item):
    """Yield the full JSON export of a trace record as text chunks.

    Spill traces stream their complete final output row by row from the
    committed spill files instead of materialising it in memory; memory
    traces keep the original whole-record JSON document.
    """
    if (item.get('mode') or 'memory') != 'spill' or item['status'] != 'completed':
        yield json.dumps(item, ensure_ascii=False, indent=2)
        return
    head = {k: v for k, v in item.items() if k != 'result'}
    head['result'] = {k: v for k, v in item['result'].items()
                      if k != 'output_preview'}
    head['result']['output'] = None
    prefix = json.dumps(head, ensure_ascii=False, indent=2)
    marker = '"output": null'
    idx = prefix.index(marker)
    yield prefix[:idx] + '"output": ['
    first = True
    for row in load_output(item['id']):
        yield ('' if first else ',') + json.dumps(
            row, ensure_ascii=False, separators=(',', ':'))
        first = False
    yield ']' + prefix[idx + len(marker):]
