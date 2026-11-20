"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

pipeline 路由（/api/pipeline/...，多段具体路径）必须先于 resource 注册，
否则 resource 的 /api/{module}/{entry_id} 会抢先匹配并按整数 entry_id 解析，
导致 pipeline 的具体 GET 端点被 422 拦截。
"""
from __future__ import annotations

from app.routers import pipeline, resource

ROUTERS = [pipeline, resource]
