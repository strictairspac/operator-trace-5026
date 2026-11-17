ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed')
OPERATORS=('scan','filter','hash_join','group')
# memory: original fully in-memory engine; spill: external engine that streams
# every intermediate stage through /artifacts with a 500-row memory ceiling
EXECUTION_MODES=('memory','spill')
