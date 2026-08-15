//! 下载引擎 — 全局单例，零依赖，同步 RPC（无 thread::spawn）
//!
//! 借鉴 aria2-rs (https://github.com/ihciah/aria2-rs) 的设计：
//!   - RPC 调用走同步阻塞 TCP（std::net::TcpStream）
//!   - 前端 1s 轮询替代后台线程
//!   - 不使用 thread::spawn（Steam 沙箱不支持）

pub mod aria2;
pub mod task;

use std::collections::HashMap;
use std::process::Child;
use std::sync::{Arc, Mutex};

use self::aria2::{spawn_aria2, Aria2Rpc};
use self::task::{DownloadTask, FileEntry, FileState, TaskState};

// ── 全局单例 ──────────────────────────────

static mut ENGINE: Option<DownloadEngine> = None;

pub fn engine() -> &'static mut DownloadEngine {
    unsafe { ENGINE.as_mut().expect("download engine not initialized") }
}

pub fn init_engine(plugin_dir: &str, work_dir: &str) -> bool {
    match DownloadEngine::new(plugin_dir, work_dir) {
        Ok(eng) => {
            unsafe { ENGINE = Some(eng); }
            true
        }
        Err(_) => false,
    }
}

// ═══════════════════════════════════════════

pub struct DownloadEngine {
    /// task_id → (gid, file_name)
    tasks: Arc<Mutex<HashMap<String, TaskEntry>>>,
    child: Arc<Mutex<Option<Child>>>,
    rpc_port: u16,
    rpc_secret: String,
    download_dir: String,
}

struct TaskEntry {
    gid: String,
    file_name: String,
    file_size: u64,
    download_url: String,
}

impl DownloadEngine {
    pub fn new(plugin_dir: &str, work_dir: &str) -> Result<Self, String> {
        let binary = format!("{}/defaults/bin/aria2c", plugin_dir);
        let (child, port, secret) = spawn_aria2(work_dir, &binary)?;

        // 等待 RPC 就绪
        let mut rpc = Aria2Rpc::new(port, &secret);
        for _ in 0..5 {
            if rpc.health().unwrap_or(false) {
                return Ok(DownloadEngine {
                    tasks: Arc::new(Mutex::new(HashMap::new())),
                    child: Arc::new(Mutex::new(Some(child))),
                    rpc_port: port,
                    rpc_secret: secret,
                    download_dir: work_dir.to_string(),
                });
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        Ok(DownloadEngine {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            child: Arc::new(Mutex::new(Some(child))),
            rpc_port: port,
            rpc_secret: secret,
            download_dir: work_dir.to_string(),
        })
    }

    /// 同步创建下载 —
    /// 借鉴 aria2-rs 的 AddUriCall 设计，
    /// uris 用 JSON 数组序列化，options 用正确 JSON 类型
    pub fn start_download(
        &self,
        urls: Vec<String>,
        names: Vec<String>,
        sizes: Vec<u64>,
        cookie: &str,
    ) -> Result<String, String> {
        if urls.is_empty() || urls.len() != names.len() {
            return Err("urls and names length mismatch".into());
        }

        let mut rpc = Aria2Rpc::new(self.rpc_port, &self.rpc_secret);

        // 同步提交 aria2（阻塞 ~1s）
        let mut gids: Vec<String> = Vec::new();
        for i in 0..urls.len() {
            let gid = rpc.add_uri(&urls[i], &self.download_dir, &names[i], cookie)?;
            gids.push(gid);
        }

        // 存储任务
        let task_id = random_hex(12);
        let mut guard = self.tasks.lock().map_err(|e| format!("lock: {}", e))?;
        for i in 0..urls.len() {
            guard.insert(
                format!("{}/{}", task_id, i),
                TaskEntry {
                    gid: gids[i].clone(),
                    file_name: names[i].clone(),
                    file_size: sizes.get(i).copied().unwrap_or(0),
                    download_url: urls[i].clone(),
                },
            );
        }

        Ok(task_id)
    }

    /// 同步轮询 —
    /// 借鉴 aria2-rs 的 TellStatusCall，返回 JSON 格式进度
    pub fn poll(&self, task_id: &str) -> Result<String, String> {
        let mut rpc = Aria2Rpc::new(self.rpc_port, &self.rpc_secret);
        let guard = self.tasks.lock().map_err(|e| format!("lock: {}", e))?;

        let mut files_json = Vec::new();
        for (key, entry) in guard.iter() {
            if key.starts_with(task_id) {
                match rpc.tell_status(&entry.gid) {
                    Ok(resp) => {
                        let status = json_field(&resp, "status");
                        let completed = json_field_u64(&resp, "completedLength");
                        let total = json_field_u64(&resp, "totalLength");
                        let speed = json_field_u64(&resp, "downloadSpeed");
                        let err_msg = json_field(&resp, "errorMessage");

                        files_json.push(format!(
                            r#"{{"name":"{}","size_bytes":{},"downloaded":{},"speed":{},"status":"{}","error":"{}"}}"#,
                            entry.file_name,
                            total.unwrap_or(entry.file_size),
                            completed.unwrap_or(0),
                            speed.unwrap_or(0),
                            status.unwrap_or_else(|| "active".into()),
                            err_msg.unwrap_or_default(),
                        ));
                    }
                    Err(e) => {
                        files_json.push(format!(
                            r#"{{"name":"{}","size_bytes":{},"downloaded":0,"speed":0,"status":"error","error":"{}"}}"#,
                            entry.file_name, entry.file_size, e,
                        ));
                    }
                }
            }
        }

        Ok(format!(
            r#"{{"task_id":"{}","files":[{}]}}"#,
            task_id,
            files_json.join(",")
        ))
    }

    pub fn cancel(&self, task_id: &str) -> Result<(), String> {
        let mut rpc = Aria2Rpc::new(self.rpc_port, &self.rpc_secret);
        let guard = self.tasks.lock().map_err(|e| format!("lock: {}", e))?;
        for (key, entry) in guard.iter() {
            if key.starts_with(task_id) {
                let _ = rpc.remove(&entry.gid);
            }
        }
        Ok(())
    }

    pub fn stop(&self) {
        if let Ok(mut guard) = self.child.lock() {
            if let Some(ref mut child) = *guard {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

// ── 极简 JSON 字段提取 ────────────────────

fn json_field(json: &str, key: &str) -> Option<String> {
    let pat = format!("\"{}\"", key);
    let pos = json.find(&pat)?;
    let after = &json[pos + pat.len()..];
    let colon = after.find(':')?;
    let val = after[colon + 1..].trim();
    if val.starts_with('"') {
        let end = val[1..].find('"')?;
        Some(val[1..=end].to_string())
    } else if val == "null" || val.starts_with("null") {
        None
    } else {
        let end = val.find(|c: char| !c.is_alphanumeric() && c != '.' && c != '-').unwrap_or(val.len());
        Some(val[..end].to_string())
    }
}

fn json_field_u64(json: &str, key: &str) -> Option<u64> {
    json_field(json, key)?.parse().ok()
}

fn random_hex(len: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{:0width$x}", t % 10u128.pow(len as u32), width = len)
}
