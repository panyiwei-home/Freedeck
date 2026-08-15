//! 解压引擎 — C-ABI 导出层
//!
//! 提供 6 个 C-ABI 函数供 Python ctypes 调用：
//!   nfc_extract_init     — 初始化（设置 plugin_dir）
//!   nfc_extract_detect   — 检测归档信息（格式、分卷、主卷）
//!   nfc_extract_start    — 启动解压
//!   nfc_extract_poll     — 轮询进度
//!   nfc_extract_cancel   — 取消解压
//!   nfc_extract_list     — 列出归档内容

pub mod detector;
pub mod runner;

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::sync::Mutex;

use self::detector::detect_archive;
use self::runner::{start_extract, list_archive, ExtractTask};

// ── 全局单例 ──────────────────────────────

static mut PLUGIN_DIR: Option<String> = None;
static mut ENGINE: Option<ExtractEngine> = None;

fn engine() -> &'static mut ExtractEngine {
    unsafe { ENGINE.as_mut().expect("extract engine not initialized") }
}

fn plugin_dir() -> &'static str {
    unsafe { PLUGIN_DIR.as_ref().expect("plugin dir not initialized") }
}

// ═══════════════════════════════════════════

pub struct ExtractEngine {
    tasks: Mutex<HashMap<String, ExtractTask>>,
}

impl ExtractEngine {
    pub fn new() -> Self {
        ExtractEngine {
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_task(&self, task: ExtractTask) -> String {
        let id = task.task_id.clone();
        if let Ok(mut guard) = self.tasks.lock() {
            guard.insert(id.clone(), task);
        }
        id
    }

    pub fn with_task<F, R>(&self, task_id: &str, f: F) -> Option<R>
    where
        F: FnOnce(&ExtractTask) -> R,
    {
        let guard = self.tasks.lock().ok()?;
        guard.get(task_id).map(f)
    }

    pub fn remove_task(&self, task_id: &str) {
        if let Ok(mut guard) = self.tasks.lock() {
            guard.remove(task_id);
        }
    }
}

// ── C-ABI 导出 ────────────────────────────

/// 初始化解压引擎（设置 plugin_dir 用于定位 7zz 二进制）
#[no_mangle]
pub extern "C" fn nfc_extract_init(plugin_dir_c: *const c_char) -> i32 {
    let dir = unsafe { cstr(plugin_dir_c) }.to_string();
    unsafe {
        PLUGIN_DIR = Some(dir);
        ENGINE = Some(ExtractEngine::new());
    }
    0
}

/// 检测归档信息
/// 输入: 归档文件路径
/// 返回: JSON 字符串（调用方需 nfc_free 释放）
///   { "format": "SevenZip", "is_split": true, "main_volume": "...", "all_volumes": [...], "volume_count": 2, "compressed_size": 12345 }
#[no_mangle]
pub extern "C" fn nfc_extract_detect(archive_path_c: *const c_char) -> *mut c_char {
    let path = unsafe { cstr(archive_path_c) };
    let p = Path::new(path);

    match detect_archive(p) {
        Some(info) => {
            let vols_json: Vec<String> = info.all_volumes.iter()
                .map(|v| format!(r#""{}""#, json_escape(&v.to_string_lossy())))
                .collect();
            let json = format!(
                r#"{{"format":"{:?}","extension":"{}","is_split":{},"main_volume":"{}","all_volumes":[{}],"volume_count":{},"compressed_size":{}}}"#,
                info.format,
                json_escape(info.format.extension()),
                info.is_split,
                json_escape(&info.main_volume.to_string_lossy()),
                vols_json.join(","),
                info.volume_count,
                info.compressed_size,
            );
            cstring(&json)
        }
        None => cstring(r#"{"error":"unable to detect archive"}"#),
    }
}

/// 启动解压
/// 输入: archive_path, output_dir, password
/// 返回: JSON 字符串（调用方需 nfc_free 释放）
///   { "task_id": "abc123", "format": "SevenZip", "is_split": true, "volume_count": 3 }
#[no_mangle]
pub extern "C" fn nfc_extract_start(
    archive_path_c: *const c_char,
    output_dir_c: *const c_char,
    password_c: *const c_char,
) -> *mut c_char {
    let archive_path = unsafe { cstr(archive_path_c) };
    let output_dir = unsafe { cstr(output_dir_c) };
    let password = unsafe { cstr(password_c) };
    let pd = plugin_dir();

    match start_extract(pd, Path::new(archive_path), Path::new(output_dir), password) {
        Ok(task) => {
            let json = format!(
                r#"{{"task_id":"{}","format":"{:?}","is_split":{},"volume_count":{},"compressed_size":{}}}"#,
                task.task_id,
                task.info.format,
                task.info.is_split,
                task.info.volume_count,
                task.info.compressed_size,
            );
            let task_id = engine().add_task(task);
            cstring(&json)
        }
        Err(e) => cstring(&format!(r#"{{"error":"{}"}}"#, json_escape(&e))),
    }
}

/// 轮询解压进度
/// 输入: task_id
/// 返回: JSON 字符串（调用方需 nfc_free 释放）
#[no_mangle]
pub extern "C" fn nfc_extract_poll(task_id_c: *const c_char) -> *mut c_char {
    let task_id = unsafe { cstr(task_id_c) };
    let eng = engine();

    let result = eng.with_task(task_id, |task| task.to_json());
    match result {
        Some(json) => cstring(&json),
        None => cstring(r#"{"error":"task not found"}"#),
    }
}

/// 取消解压
/// 输入: task_id
/// 返回: 0 = 成功, 1 = 任务不存在
#[no_mangle]
pub extern "C" fn nfc_extract_cancel(task_id_c: *const c_char) -> i32 {
    let task_id = unsafe { cstr(task_id_c) };
    let eng = engine();

    let result = eng.with_task(task_id, |task| { task.cancel(); });
    match result {
        Some(()) => 0,
        None => 1,
    }
}

/// 列出归档内容
/// 输入: archive_path, password
/// 返回: JSON 数组字符串（调用方需 nfc_free 释放）
#[no_mangle]
pub extern "C" fn nfc_extract_list(
    archive_path_c: *const c_char,
    password_c: *const c_char,
) -> *mut c_char {
    let archive_path = unsafe { cstr(archive_path_c) };
    let password = unsafe { cstr(password_c) };
    let pd = plugin_dir();

    match list_archive(pd, Path::new(archive_path), password) {
        Ok(entries) => {
            let items: Vec<String> = entries.iter().map(|e| {
                format!(
                    r#"{{"path":"{}","size":{},"compressed":{},"is_directory":{},"modified":"{}"}}"#,
                    json_escape(&e.path),
                    e.size,
                    e.compressed_size,
                    e.is_directory,
                    json_escape(&e.modified),
                )
            }).collect();
            cstring(&format!(r#"[{}]"#, items.join(",")))
        }
        Err(e) => cstring(&format!(r#"{{"error":"{}"}}"#, json_escape(&e))),
    }
}

// ── 工具函数 ──────────────────────────────

unsafe fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() { return ""; }
    CStr::from_ptr(p).to_str().unwrap_or("")
}

fn cstring(s: &str) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

fn json_escape(s: &str) -> String {
    let mut r = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => r.push_str("\\\""),
            '\\' => r.push_str("\\\\"),
            '\n' => r.push_str("\\n"),
            '\r' => r.push_str("\\r"),
            '\t' => r.push_str("\\t"),
            c => r.push(c),
        }
    }
    r
}
