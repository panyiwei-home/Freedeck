# tianyi_http.py - 天翼下载 HTTP 接口与页面路由
#
# 该模块只处理 HTTP 请求与响应格式。

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import decky
from aiohttp import web

import config
from tianyi_service import LocalWebNotReadyError


def _json_ok(data: Optional[Dict[str, Any]] = None) -> web.Response:
    """返回统一成功响应。"""
    payload: Dict[str, Any] = {"status": "success"}
    if data:
        payload.update(data)
    return web.json_response(payload)


def _json_error(message: str, status: int = 400, *, reason: str = "", diagnostics: Optional[Dict[str, Any]] = None) -> web.Response:
    """返回统一错误响应。"""
    payload: Dict[str, Any] = {"status": "error", "message": str(message)}
    if reason:
        payload["reason"] = str(reason)
    if diagnostics is not None:
        payload["diagnostics"] = diagnostics
    return web.json_response(payload, status=status)


def _json_error_from_exception(exc: Exception, status: int = 400) -> web.Response:
    """把异常转换为统一错误响应，并尽量透传诊断信息。"""
    diagnostics = getattr(exc, "diagnostics", None)
    if not isinstance(diagnostics, dict):
        diagnostics = None
    try:
        config.logger.exception("Tianyi HTTP error status=%s exc=%s diagnostics=%s", status, exc, diagnostics)
    except Exception:
        pass
    return _json_error(str(exc), status=status, diagnostics=diagnostics)


def _ui_root() -> Optional[Path]:
    """解析游戏库页面静态目录。"""
    plugin_dir = getattr(decky, "DECKY_PLUGIN_DIR", None)
    if not plugin_dir:
        return None
    root = Path(plugin_dir).resolve() / "defaults" / "tianyi_library_ui"
    if root.is_dir():
        return root
    return None


def _safe_asset_path(root: Path, req_path: str) -> Optional[Path]:
    """安全解析静态资源路径，防止越界读取。"""
    normalized = (req_path or "").replace("\\", "/").strip("/")
    if not normalized:
        normalized = "index.html"
    target = (root / normalized).resolve()
    try:
        if os.path.commonpath([str(root), str(target)]) != str(root):
            return None
    except Exception:
        return None
    if not target.is_file():
        return None
    return target


def _service(plugin: Any):
    service = getattr(plugin, "tianyi_service", None)
    if service is None:
        raise RuntimeError("天翼服务未初始化")
    return service


async def _serve_ui_file(filename: str) -> web.StreamResponse:
    root = _ui_root()
    if root is None:
        return _json_error("未找到游戏库页面资源，请确认 defaults/tianyi_library_ui", 503)
    target = _safe_asset_path(root, filename)
    if target is None:
        return _json_error(f"页面资源缺失: {filename}", 503)
    return web.FileResponse(target)


async def handle_library_index(request: web.Request, plugin: Any) -> web.StreamResponse:
    """返回游戏库首页。"""
    return await _serve_ui_file("index.html")


async def handle_login_bridge_index(request: web.Request, plugin: Any) -> web.StreamResponse:
    """返回登录桥接页。"""
    return await _serve_ui_file("login_bridge.html")


async def handle_library_static(request: web.Request, plugin: Any) -> web.StreamResponse:
    """返回游戏库静态资源。"""
    root = _ui_root()
    if root is None:
        return _json_error("未找到游戏库静态资源", 503)
    req_path = request.match_info.get("path", "")
    target = _safe_asset_path(root, req_path)
    if target is None:
        # 前端路由回退到 index.html
        target = _safe_asset_path(root, "index.html")
        if target is None:
            return _json_error("页面资源不存在", 404)
    return web.FileResponse(target)


async def handle_state(request: web.Request, plugin: Any) -> web.Response:
    """获取面板状态。"""
    try:
        data = await _service(plugin).get_panel_state()
        tasks = data.get("tasks", []) if isinstance(data, dict) else []
        plan = data.get("plan", {}) if isinstance(data, dict) else {}
        return _json_ok({"data": {"tasks": tasks, "plan": plan}})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_check(request: web.Request, plugin: Any) -> web.Response:
    """校验登录态。"""
    try:
        ok, account, message = await _service(plugin).check_login_state()
        return _json_ok({"data": {"logged_in": ok, "user_account": account, "message": message}})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_manual(request: web.Request, plugin: Any) -> web.Response:
    """手动保存 cookie。"""
    try:
        body = await request.json()
        cookie = str(body.get("cookie", "")).strip()
        user_account = str(body.get("user_account", "")).strip()
        data = await _service(plugin).save_manual_cookie(cookie, user_account)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_login_clear(request: web.Request, plugin: Any) -> web.Response:
    """清理登录态。"""
    try:
        data = await _service(plugin).clear_login()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_capture_start(request: web.Request, plugin: Any) -> web.Response:
    """启动登录态自动采集。"""
    try:
        body = await request.json()
    except Exception:
        body = {}

    try:
        timeout_seconds = int(body.get("timeout_seconds", 240))
    except Exception:
        timeout_seconds = 240

    try:
        data = await _service(plugin).start_login_capture(timeout_seconds=timeout_seconds)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_capture_status(request: web.Request, plugin: Any) -> web.Response:
    """查询登录态自动采集状态。"""
    try:
        data = await _service(plugin).get_login_capture_status()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_capture_stop(request: web.Request, plugin: Any) -> web.Response:
    """停止登录态自动采集。"""
    try:
        data = await _service(plugin).stop_login_capture()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_qr_start(request: web.Request, plugin: Any) -> web.Response:
    """启动二维码登录。"""
    try:
        data = await _service(plugin).start_qr_login()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_qr_status(request: web.Request, plugin: Any) -> web.Response:
    """轮询二维码登录状态。"""
    session_id = str(request.query.get("session_id", "")).strip()
    poll_flag = str(request.query.get("poll", "1")).strip().lower()
    try:
        if poll_flag in {"0", "false", "no"}:
            data = await _service(plugin).get_qr_login_state()
        else:
            data = await _service(plugin).poll_qr_login(session_id=session_id)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_qr_stop(request: web.Request, plugin: Any) -> web.Response:
    """停止二维码登录。"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    session_id = str(body.get("session_id", "")).strip()

    try:
        data = await _service(plugin).stop_qr_login(session_id=session_id)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_login_qr_image(request: web.Request, plugin: Any) -> web.Response:
    """返回二维码图片。"""
    session_id = str(request.query.get("session_id", "")).strip()
    try:
        data, content_type = await _service(plugin).get_qr_login_image(session_id=session_id)
        return web.Response(body=data, content_type=content_type)
    except Exception as exc:
        return _json_error_from_exception(exc, 400)


async def handle_login_redirect(request: web.Request, plugin: Any) -> web.Response:
    """重定向到本地登录桥接页。"""
    try:
        url = await _service(plugin).get_login_url()
        raise web.HTTPFound(url)
    except LocalWebNotReadyError as exc:
        return _json_error(str(exc), 503, reason=exc.reason, diagnostics=exc.diagnostics)
    except web.HTTPException:
        raise
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_catalog(request: web.Request, plugin: Any) -> web.Response:
    """查询目录列表。"""
    try:
        query = str(request.query.get("q", "")).strip()
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", request.query.get("pageSize", "0")))
        data = await _service(plugin).list_catalog(query, page, page_size)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_catalog_cover(request: web.Request, plugin: Any) -> web.Response:
    """按标题解析游戏封面。"""
    try:
        game_id = str(request.query.get("game_id", "")).strip()
        title = str(request.query.get("title", "")).strip()
        categories = str(request.query.get("categories", "")).strip()
        data = await _service(plugin).resolve_catalog_cover(game_id=game_id, title=title, categories=categories)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_settings_get(request: web.Request, plugin: Any) -> web.Response:
    """读取设置。"""
    try:
        data = await _service(plugin).get_settings()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_settings_set(request: web.Request, plugin: Any) -> web.Response:
    """保存设置。"""
    try:
        body = await request.json()
        data = await _service(plugin).update_settings(
            download_dir=body.get("download_dir"),
            install_dir=body.get("install_dir"),
            split_count=body.get("split_count"),
            page_size=body.get("page_size"),
            auto_delete_package=body.get("auto_delete_package"),
            auto_install=body.get("auto_install"),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_upload_start(request: web.Request, plugin: Any) -> web.Response:
    """启动云存档上传任务。"""
    try:
        data = await _service(plugin).start_cloud_save_upload()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_upload_status(request: web.Request, plugin: Any) -> web.Response:
    """查询云存档上传任务状态。"""
    try:
        data = await _service(plugin).get_cloud_save_upload_status()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_cloud_save_restore_list(request: web.Request, plugin: Any) -> web.Response:
    """查询可恢复云存档版本。"""
    try:
        data = await _service(plugin).list_cloud_save_restore_options()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_restore_entries(request: web.Request, plugin: Any) -> web.Response:
    """读取指定版本可选存档项。"""
    try:
        body = await request.json()
        data = await _service(plugin).list_cloud_save_restore_entries(
            game_id=str(body.get("game_id", "")).strip(),
            game_key=str(body.get("game_key", "")).strip(),
            game_title=str(body.get("game_title", "")).strip(),
            version_name=str(body.get("version_name", "")).strip(),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_restore_plan(request: web.Request, plugin: Any) -> web.Response:
    """生成云存档恢复计划（冲突探测）。"""
    try:
        body = await request.json()
        selected_entry_ids = body.get("selected_entry_ids", [])
        if not isinstance(selected_entry_ids, list):
            selected_entry_ids = []
        data = await _service(plugin).plan_cloud_save_restore(
            game_id=str(body.get("game_id", "")).strip(),
            game_key=str(body.get("game_key", "")).strip(),
            game_title=str(body.get("game_title", "")).strip(),
            version_name=str(body.get("version_name", "")).strip(),
            selected_entry_ids=[str(item) for item in selected_entry_ids if str(item).strip()],
            target_dir=str(body.get("target_dir", "")).strip(),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_restore_apply(request: web.Request, plugin: Any) -> web.Response:
    """执行云存档恢复计划。"""
    try:
        body = await request.json()
        data = await _service(plugin).apply_cloud_save_restore(
            plan_id=str(body.get("plan_id", "")).strip(),
            confirm_overwrite=bool(body.get("confirm_overwrite", False)),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_cloud_save_restore_status(request: web.Request, plugin: Any) -> web.Response:
    """读取云存档恢复任务状态。"""
    try:
        data = await _service(plugin).get_cloud_save_restore_status()
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_create_download(request: web.Request, plugin: Any) -> web.Response:
    """创建下载任务。"""
    try:
        body = await request.json()
        game_id = str(body.get("game_id", "")).strip()
        share_url = str(body.get("share_url", "")).strip()
        file_ids = body.get("file_ids", [])
        if not isinstance(file_ids, list):
            file_ids = []
        data = await _service(plugin).start_install(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            split_count=body.get("split_count"),
            download_dir=body.get("download_dir"),
            install_dir=body.get("install_dir"),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_prepare_install(request: web.Request, plugin: Any) -> web.Response:
    """安装前探针与确认数据。"""
    try:
        body = await request.json()
        game_id = str(body.get("game_id", "")).strip()
        share_url = str(body.get("share_url", "")).strip()
        file_ids = body.get("file_ids", [])
        if not isinstance(file_ids, list):
            file_ids = []
        data = await _service(plugin).prepare_install(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            download_dir=body.get("download_dir"),
            install_dir=body.get("install_dir"),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_start_install(request: web.Request, plugin: Any) -> web.Response:
    """确认后开始下载与安装流程。"""
    try:
        body = await request.json()
        game_id = str(body.get("game_id", "")).strip()
        share_url = str(body.get("share_url", "")).strip()
        file_ids = body.get("file_ids", [])
        if not isinstance(file_ids, list):
            file_ids = []
        data = await _service(plugin).start_install(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            split_count=body.get("split_count"),
            download_dir=body.get("download_dir"),
            install_dir=body.get("install_dir"),
        )
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_tasks(request: web.Request, plugin: Any) -> web.Response:
    """查询任务状态。"""
    try:
        tasks = await _service(plugin).refresh_tasks(sync_aria2=True)
        return _json_ok({"data": {"tasks": tasks}})
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


async def handle_pause(request: web.Request, plugin: Any) -> web.Response:
    """暂停任务。"""
    try:
        body = await request.json()
        task_id = str(body.get("task_id", "")).strip()
        data = await _service(plugin).pause_task(task_id)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_resume(request: web.Request, plugin: Any) -> web.Response:
    """恢复任务。"""
    try:
        body = await request.json()
        task_id = str(body.get("task_id", "")).strip()
        data = await _service(plugin).resume_task(task_id)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_remove(request: web.Request, plugin: Any) -> web.Response:
    """移除任务。"""
    try:
        body = await request.json()
        task_id = str(body.get("task_id", "")).strip()
        data = await _service(plugin).remove_task(task_id)
        return _json_ok({"data": data})
    except Exception as exc:
        return _json_error_from_exception(exc)


async def handle_login_url(request: web.Request, plugin: Any) -> web.Response:
    """获取本地登录桥接地址。"""
    try:
        url = await _service(plugin).get_login_url()
        return _json_ok({"data": {"url": url}, "url": url})
    except LocalWebNotReadyError as exc:
        return _json_error(str(exc), 503, reason=exc.reason, diagnostics=exc.diagnostics)
    except Exception as exc:
        return _json_error_from_exception(exc, 500)


def setup_routes(app: web.Application, plugin: Any) -> None:
    """挂载天翼相关路由。"""
    # 本地网页与静态资源
    app.router.add_get("/tianyi/library", lambda request: handle_library_index(request, plugin))
    app.router.add_get("/tianyi/library/login-bridge", lambda request: handle_login_bridge_index(request, plugin))
    app.router.add_get("/tianyi/library/{path:.*}", lambda request: handle_library_static(request, plugin))

    # 业务 API
    app.router.add_get("/api/tianyi/state", lambda request: handle_state(request, plugin))
    app.router.add_get("/api/tianyi/login-url", lambda request: handle_login_url(request, plugin))
    app.router.add_get("/api/tianyi/login", lambda request: handle_login_redirect(request, plugin))
    app.router.add_post("/api/tianyi/login/check", lambda request: handle_login_check(request, plugin))
    app.router.add_post("/api/tianyi/login/manual", lambda request: handle_login_manual(request, plugin))
    app.router.add_post("/api/tianyi/login/clear", lambda request: handle_login_clear(request, plugin))

    app.router.add_post("/api/tianyi/login/capture/start", lambda request: handle_login_capture_start(request, plugin))
    app.router.add_get("/api/tianyi/login/capture/status", lambda request: handle_login_capture_status(request, plugin))
    app.router.add_post("/api/tianyi/login/capture/stop", lambda request: handle_login_capture_stop(request, plugin))
    app.router.add_post("/api/tianyi/login/qr/start", lambda request: handle_login_qr_start(request, plugin))
    app.router.add_get("/api/tianyi/login/qr/status", lambda request: handle_login_qr_status(request, plugin))
    app.router.add_post("/api/tianyi/login/qr/stop", lambda request: handle_login_qr_stop(request, plugin))
    app.router.add_get("/api/tianyi/login/qr/image", lambda request: handle_login_qr_image(request, plugin))

    app.router.add_get("/api/tianyi/catalog", lambda request: handle_catalog(request, plugin))
    app.router.add_get("/api/tianyi/catalog/cover", lambda request: handle_catalog_cover(request, plugin))
    app.router.add_get("/api/tianyi/settings", lambda request: handle_settings_get(request, plugin))
    app.router.add_post("/api/tianyi/settings", lambda request: handle_settings_set(request, plugin))
    app.router.add_post("/api/tianyi/cloud-save/upload/start", lambda request: handle_cloud_save_upload_start(request, plugin))
    app.router.add_get("/api/tianyi/cloud-save/upload/status", lambda request: handle_cloud_save_upload_status(request, plugin))
    app.router.add_get("/api/tianyi/cloud-save/restore/list", lambda request: handle_cloud_save_restore_list(request, plugin))
    app.router.add_post("/api/tianyi/cloud-save/restore/entries", lambda request: handle_cloud_save_restore_entries(request, plugin))
    app.router.add_post("/api/tianyi/cloud-save/restore/plan", lambda request: handle_cloud_save_restore_plan(request, plugin))
    app.router.add_post("/api/tianyi/cloud-save/restore/apply", lambda request: handle_cloud_save_restore_apply(request, plugin))
    app.router.add_get("/api/tianyi/cloud-save/restore/status", lambda request: handle_cloud_save_restore_status(request, plugin))

    app.router.add_post("/api/tianyi/install/prepare", lambda request: handle_prepare_install(request, plugin))
    app.router.add_post("/api/tianyi/install/start", lambda request: handle_start_install(request, plugin))
    app.router.add_post("/api/tianyi/download/create", lambda request: handle_create_download(request, plugin))
    app.router.add_get("/api/tianyi/download/tasks", lambda request: handle_tasks(request, plugin))
    app.router.add_post("/api/tianyi/download/pause", lambda request: handle_pause(request, plugin))
    app.router.add_post("/api/tianyi/download/resume", lambda request: handle_resume(request, plugin))
    app.router.add_post("/api/tianyi/download/remove", lambda request: handle_remove(request, plugin))
