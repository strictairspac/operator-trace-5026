export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待执行',running:'执行中',completed:'已完成',failed:'执行失败'};
export const OPERATORS=['scan','filter','hash_join','group'];
export const EXECUTION_MODES={memory:'内存模式（上限1万连接行）',spill:'外排模式（上限20万连接行，内存封顶500行）'};
