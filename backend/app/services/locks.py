"""业务互斥锁：同一检查点的恢复受理、清单流转与作业侧消费必须串行。

内存仓库本身不是线程安全的，FastAPI 的同步路由又运行在线程池里。把恢复链路用一把
可重入锁串起来，才能保证「同一检查点只发放一把恢复锁」「清单与路径引用一起回退」
这类跨两张表的操作不会被并发请求从中间插进来。
"""
from __future__ import annotations

import threading

RESTORE_MUTEX = threading.RLock()
