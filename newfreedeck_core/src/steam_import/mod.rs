//! Steam 导入引擎 — C-ABI 导出层
//!
//! 3 个 C-ABI 函数：
//!   nfc_steam_add_shortcut — 调用 steamos-add-to-steam
//!   nfc_steam_discover_exe — 从解压目录匹配 exe
//!   nfc_steam_get_steam_id — 读取 loginusers.vdf 获取 steam_id

pub mod discover;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::process::Command;

use self::discover::discover_exe;

// ── C-ABI 导出 ────────────────────────────

/// 调用 steamos-add-to-steam <exe> 导入 Steam
/// 返回 JSON: {"success": true, "method": "steamos-add-to-steam"}
#[no_mangle]
pub extern "C" fn nfc_steam_add_shortcut(exe_path_c: *const c_char) -> *mut c_char {
    let exe_path = unsafe { cstr(exe_path_c) };

    if exe_path.is_empty() || !Path::new(exe_path).exists() {
        return cstring(r#"{"success":false,"error":"exe not found"}"#);
    }

    // 尝试 steamos-add-to-steam
    match Command::new("steamos-add-to-steam")
        .arg(exe_path)
        .output()
    {
        Ok(output) => {
            if output.status.success() {
                cstring(r#"{"success":true,"method":"steamos-add-to-steam"}"#)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                cstring(&format!(r#"{{"success":false,"error":"{}","method":"steamos-add-to-steam"}}"#, json_escape(&stderr)))
            }
        }
        Err(e) => {
            // steamos-add-to-steam 不可用，记录错误
            cstring(&format!(r#"{{"success":false,"error":"spawn failed: {}","method":"none"}}"#, json_escape(&e.to_string())))
        }
    }
}

/// 从解压目录发现 exe
/// 输入: extract_dir, openpath (可为空)
/// 返回 JSON: {"exe_path":"...", "candidates":[...], "method":"openpath", "found":true}
#[no_mangle]
pub extern "C" fn nfc_steam_discover_exe(
    extract_dir_c: *const c_char,
    openpath_c: *const c_char,
) -> *mut c_char {
    let extract_dir = unsafe { cstr(extract_dir_c) };
    let openpath = unsafe { cstr(openpath_c) };

    let result = discover_exe(Path::new(extract_dir), openpath);

    let candidates_json: Vec<String> = result
        .candidates
        .iter()
        .map(|c| format!(r#""{}""#, json_escape(c)))
        .collect();

    let json = format!(
        r#"{{"exe_path":"{}","candidates":[{}],"method":"{}","found":{}}}"#,
        json_escape(&result.exe_path),
        candidates_json.join(","),
        json_escape(&result.method),
        result.found,
    );
    cstring(&json)
}

/// 获取当前活跃 Steam 用户 ID (SteamID64)
///
/// 从 ~/.local/share/Steam/config/loginusers.vdf 读取，
/// 取 mostrecent=1 的用户。
///
/// 返回 JSON: {"steam_id": "76561198012345678"}
/// 或: {"error": "..."}
#[no_mangle]
pub extern "C" fn nfc_steam_get_steam_id() -> *mut c_char {
    // 确定 Steam 用户数据目录
    let home = std::env::var("DECKY_USER_HOME")
        .unwrap_or_else(|_| std::env::var("HOME").unwrap_or_else(|_| "/home/deck".into()));

    let loginusers_path = Path::new(&home)
        .join(".local")
        .join("share")
        .join("Steam")
        .join("config")
        .join("loginusers.vdf");

    let content = match std::fs::read_to_string(&loginusers_path) {
        Ok(c) => c,
        Err(e) => return cstring(&format!(r#"{{"error":"read loginusers.vdf: {}"}}"#, json_escape(&e.to_string()))),
    };

    // 简单解析：找 mostrecent=1 的用户块
    // loginusers.vdf 格式（Text VDF）：
    // "users" {
    //   "76561198012345678" {
    //     "AccountName" "deck"
    //     "mostrecent" "1"
    //     ...
    //   }
    // }

    let mut current_steam_id = String::new();
    let mut found_mostrecent = false;

    let mut lines = content.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        // 匹配用户 ID 行: "76561198012345678" {
        if trimmed.starts_with('"') && trimmed.ends_with('{') {
            let id_candidate = trimmed[1..].split('"').next().unwrap_or("");
            if id_candidate.len() == 17 && id_candidate.chars().all(|c| c.is_ascii_digit()) {
                // 进入该用户块，寻找 mostrecent
                let mut is_mostrecent = false;
                for inner_line in lines.by_ref() {
                    let t = inner_line.trim();
                    if t == "}" {
                        break; // 用户块结束
                    }
                    if t.contains("mostrecent") && (t.contains("\"1\"") || t.contains("1")) {
                        is_mostrecent = true;
                    }
                }
                if is_mostrecent {
                    current_steam_id = id_candidate.to_string();
                    found_mostrecent = true;
                    break;
                }
            }
        }
    }

    // 确定最终返回的 steam_id（含 userdata 目录验证）
    let resolve_id = |id: &str| -> Option<String> {
        let userdata_base = Path::new(&home)
            .join(".local").join("share").join("Steam").join("userdata");

        // 优先检查 id 对应的目录
        let sp = userdata_base.join(id).join("config").join("shortcuts.vdf");
        if sp.exists() {
            return Some(id.to_string());
        }

        // 回退：检查 userdata/0/（Steam Deck 匿名用户/离线模式）
        let sp0 = userdata_base.join("0").join("config").join("shortcuts.vdf");
        if sp0.exists() {
            return Some("0".to_string());
        }

        // 回退：检查 userdata 下第一个有 shortcuts.vdf 的目录
        if let Ok(entries) = std::fs::read_dir(&userdata_base) {
            for e in entries.flatten() {
                let p = e.path().join("config").join("shortcuts.vdf");
                if p.exists() {
                    if let Some(name) = e.file_name().to_str() {
                        return Some(name.to_string());
                    }
                }
            }
        }

        // 最后：即使目录不存在也返回 id（可能是首次使用）
        if !id.is_empty() {
            return Some(id.to_string());
        }
        None
    };

    if found_mostrecent {
        if let Some(resolved) = resolve_id(&current_steam_id) {
            return cstring(&format!(r#"{{"steam_id":"{}"}}"#, json_escape(&resolved)));
        }
    }

    // 回退：取第一个 17 位纯数字 key
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('"') {
            let id = trimmed[1..].split('"').next().unwrap_or("");
            if id.len() == 17 && id.chars().all(|c| c.is_ascii_digit()) {
                if let Some(resolved) = resolve_id(id) {
                    return cstring(&format!(r#"{{"steam_id":"{}"}}"#, json_escape(&resolved)));
                }
            }
        }
    }

    // 最终回退
    let sp0 = Path::new(&home)
        .join(".local").join("share").join("Steam").join("userdata")
        .join("0").join("config").join("shortcuts.vdf");
    if sp0.exists() {
        cstring(r#"{"steam_id":"0"}"#)
    } else {
        cstring(r#"{"error":"no steam userdata found"}"#)
    }
}

// ── 工具函数 ──────────────────────────────

unsafe fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
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
