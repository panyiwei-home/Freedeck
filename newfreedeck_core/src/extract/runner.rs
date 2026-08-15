//! 解压执行器 — 7zz 进程管理 + 进度解析 + 取消控制
//!
//! 参考：
//!   - 7-Zip CLI 文档 (https://7-zip.org/)
//!   - 本项目 aria2.rs 的进程 spawn 模式
//!
//! 零外部依赖 — 纯 std::process + std::io

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::detector::{ArchiveFormat, ArchiveInfo, detect_archive};

// ── 解压任务状态 ──────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractState {
    Pending,
    Running,
    Complete,
    Failed(String),
    Cancelled,
}

/// 解压任务（内部）
pub struct ExtractTask {
    pub task_id: String,
    pub archive_path: PathBuf,
    pub output_dir: PathBuf,
    pub info: ArchiveInfo,
    pub state: Arc<Mutex<ExtractState>>,
    pub progress: Arc<Mutex<f64>>,       // 0.0 ~ 100.0
    pub current_file: Arc<Mutex<String>>,
    pub error_message: Arc<Mutex<String>>,
    pub started_at: Instant,
    child: Arc<Mutex<Option<Child>>>,
}

impl ExtractTask {
    pub fn new(
        task_id: &str,
        archive_path: &Path,
        output_dir: &Path,
        info: ArchiveInfo,
        child: Child,
    ) -> Self {
        ExtractTask {
            task_id: task_id.to_string(),
            archive_path: archive_path.to_path_buf(),
            output_dir: output_dir.to_path_buf(),
            info,
            state: Arc::new(Mutex::new(ExtractState::Running)),
            progress: Arc::new(Mutex::new(0.0)),
            current_file: Arc::new(Mutex::new(String::new())),
            error_message: Arc::new(Mutex::new(String::new())),
            started_at: Instant::now(),
            child: Arc::new(Mutex::new(Some(child))),
        }
    }

    /// 取消解压：kill 子进程
    pub fn cancel(&self) {
        if let Ok(mut guard) = self.child.lock() {
            if let Some(ref mut child) = guard.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Ok(mut s) = self.state.lock() {
            *s = ExtractState::Cancelled;
        }
    }

    /// 轮询进度（读取 stdout 中的最新百分比）
    pub fn poll(&self) -> ExtractProgress {
        let state = self.state.lock().unwrap().clone();
        let progress = *self.progress.lock().unwrap();
        let current_file = self.current_file.lock().unwrap().clone();
        let error = self.error_message.lock().unwrap().clone();

        // 检查子进程是否已退出
        let child_exited = {
            let mut child_guard = self.child.lock().unwrap();
            if let Some(ref mut child) = child_guard.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        // 进程已退出 — 释放 child lock 再更新状态
                        drop(child_guard);
                        if status.success() {
                            if let Ok(mut s) = self.state.lock() {
                                if *s == ExtractState::Running {
                                    *s = ExtractState::Complete;
                                }
                            }
                            if let Ok(mut p) = self.progress.lock() {
                                *p = 100.0;
                            }
                        } else if state != ExtractState::Cancelled {
                            let code = status.code().unwrap_or(-1);
                            let msg = format!("7zz exit code {}", code);
                            if let Ok(mut s) = self.state.lock() {
                                if *s == ExtractState::Running {
                                    *s = ExtractState::Failed(msg.clone());
                                }
                            }
                            if let Ok(mut e) = self.error_message.lock() {
                                if e.is_empty() { *e = msg; }
                            }
                        }
                        if let Ok(mut g) = self.child.lock() {
                            *g = None;
                        }
                        true
                    }
                    Ok(None) => false,
                    Err(_) => {
                        drop(child_guard);
                        if let Ok(mut g) = self.child.lock() {
                            *g = None;
                        }
                        true
                    }
                }
            } else {
                true // 已经清理过
            }
        };

        let final_state = if child_exited {
            let s = self.state.lock().unwrap().clone();
            if s == ExtractState::Running { ExtractState::Complete } else { s }
        } else {
            state
        };

        let final_progress = if final_state == ExtractState::Complete { 100.0 } else { progress };

        ExtractProgress {
            task_id: self.task_id.clone(),
            state: final_state,
            progress: final_progress,
            current_file,
            error: error,
            elapsed_secs: self.started_at.elapsed().as_secs(),
            archive_format: format!("{:?}", self.info.format),
            is_split: self.info.is_split,
            volume_count: self.info.volume_count as u32,
            compressed_size: self.info.compressed_size,
        }
    }

    pub fn to_json(&self) -> String {
        let p = self.poll();
        p.to_json()
    }
}

// ── 进度快照 ──────────────────────────────

#[derive(Debug, Clone)]
pub struct ExtractProgress {
    pub task_id: String,
    pub state: ExtractState,
    pub progress: f64,
    pub current_file: String,
    pub error: String,
    pub elapsed_secs: u64,
    pub archive_format: String,
    pub is_split: bool,
    pub volume_count: u32,
    pub compressed_size: u64,
}

impl ExtractProgress {
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"task_id":"{}","state":"{}","progress":{},"current_file":"{}","error":"{}","elapsed_secs":{},"archive_format":"{}","is_split":{},"volume_count":{},"compressed_size":{}}}"#,
            self.task_id,
            state_to_str(&self.state),
            self.progress,
            json_escape(&self.current_file),
            json_escape(&self.error),
            self.elapsed_secs,
            json_escape(&self.archive_format),
            self.is_split,
            self.volume_count,
            self.compressed_size,
        )
    }
}

fn state_to_str(s: &ExtractState) -> &str {
    match s {
        ExtractState::Pending => "pending",
        ExtractState::Running => "running",
        ExtractState::Complete => "complete",
        ExtractState::Failed(_) => "failed",
        ExtractState::Cancelled => "cancelled",
    }
}

// ── 7zz 二进制路径 ───────────────────────

fn find_7zz(plugin_dir: &str) -> Option<PathBuf> {
    let candidates = [
        format!("{}/defaults/bin/7zz", plugin_dir),
        format!("{}/defaults/bin/7z", plugin_dir),
        "7zz".to_string(),
        "7z".to_string(),
    ];
    for c in &candidates {
        let p = Path::new(c);
        if p.exists() {
            return Some(p.to_path_buf());
        }
    }
    None
}

// ── 启动解压 ──────────────────────────────

/// 启动 7zz 解压进程，返回 ExtractTask。
///
/// 7zz 对于分卷归档会自动查找同目录下的其他分卷，
/// 只需传入主卷/首卷即可。
pub fn start_extract(
    plugin_dir: &str,
    archive_path: &Path,
    output_dir: &Path,
    password: &str,
) -> Result<ExtractTask, String> {
    let binary = find_7zz(plugin_dir).ok_or_else(|| "7zz binary not found".to_string())?;

    // 确保输出目录存在
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("create output dir: {}", e))?;

    // 检测归档
    let info = detect_archive(archive_path)
        .ok_or_else(|| "unable to detect archive format".to_string())?;

    if info.format == ArchiveFormat::Unknown {
        return Err("unknown archive format".into());
    }

    // 构建命令行参数
    // 对于分卷，使用主卷路径
    let archive_to_extract = if info.is_split {
        &info.main_volume
    } else {
        archive_path
    };

    let mut cmd = Command::new(&binary);
    cmd.arg("x")                                    // extract with full paths
       .arg(archive_to_extract)
       .arg(format!("-o{}", output_dir.display()))  // output dir
       .arg("-bsp1")                                 // progress to stdout
       .arg("-y")                                    // yes to all
       .arg("-aoa")                                  // overwrite all existing
       .arg("-sccUTF-8");                            // UTF-8 console output

    if !password.is_empty() {
        cmd.arg(format!("-p{}", password));
    }

    cmd.stdout(Stdio::piped())
       .stderr(Stdio::piped())
       .stdin(Stdio::null());

    let mut child = cmd.spawn().map_err(|e| format!("spawn 7zz: {}", e))?;

    let stdout = child.stdout.take()
        .ok_or_else(|| "failed to capture stdout".to_string())?;
    let stderr = child.stderr.take()
        .ok_or_else(|| "failed to capture stderr".to_string())?;

    let task_id = random_hex(12);
    let task = ExtractTask::new(&task_id, archive_path, output_dir, info, child);

    // 启动 stdout 读取线程（每读到一行更新进度）
    let progress = Arc::clone(&task.progress);
    let current_file = Arc::clone(&task.current_file);
    let error_msg = Arc::clone(&task.error_message);
    let child_arc = Arc::clone(&task.child);

    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => {
                    // 解析 -bsp1 输出
                    // 格式：百分号 空格 文件名
                    //   0%  - filename
                    //  45% 34 - filename
                    // 或简单百分比行
                    let trimmed = line.trim();
                    if trimmed.is_empty() { continue; }

                    // 尝试解析百分比
                    if let Some(pct) = parse_bsp1_progress(trimmed) {
                        if let Ok(mut p) = progress.lock() {
                            *p = pct;
                        }
                    }

                    // 提取当前文件名（-bsp1 中在 "- " 之后的部分）
                    if let Some(fname) = extract_filename_from_bsp1(trimmed) {
                        if let Ok(mut f) = current_file.lock() {
                            *f = fname;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        // stdout 关闭 = 进程结束或已关闭
        // 标记进程已结束（如果还在运行状态）
        if let Ok(mut g) = child_arc.lock() {
            g.take(); // 丢弃 child handle，标记为已处理
        }
    });

    // stderr 读取线程（错误记录）
    let err_arc = Arc::clone(&error_msg);
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        let mut errors = String::new();
        for line in reader.lines() {
            if let Ok(line) = line {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    if !errors.is_empty() { errors.push('\n'); }
                    errors.push_str(trimmed);
                }
            }
        }
        if !errors.is_empty() {
            if let Ok(mut e) = err_arc.lock() {
                if e.is_empty() { *e = errors; }
            }
        }
    });

    Ok(task)
}

/// 列出归档内容（文件清单）
pub fn list_archive(
    plugin_dir: &str,
    archive_path: &Path,
    password: &str,
) -> Result<Vec<ListEntry>, String> {
    let binary = find_7zz(plugin_dir).ok_or_else(|| "7zz binary not found".to_string())?;

    let mut cmd = Command::new(&binary);
    cmd.arg("l")
       .arg(archive_path)
       .arg("-slt")         // 技术列表格式
       .arg("-sccUTF-8")
       .arg("-y");

    if !password.is_empty() {
        cmd.arg(format!("-p{}", password));
    }

    cmd.stdout(Stdio::piped())
       .stderr(Stdio::null())
       .stdin(Stdio::null());

    let output = cmd.output().map_err(|e| format!("spawn 7zz list: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("7zz list failed: {}", stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_slt_output(&stdout)
}

/// -slt 格式输出的条目
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub path: String,
    pub size: u64,
    pub compressed_size: u64,
    pub is_directory: bool,
    pub modified: String,
}

/// 解析 7zz l -slt 输出
fn parse_slt_output(output: &str) -> Result<Vec<ListEntry>, String> {
    let mut entries: Vec<ListEntry> = Vec::new();
    let mut current_path = String::new();
    let mut current_size: u64 = 0;
    let mut current_compressed: u64 = 0;
    let mut current_is_dir = false;
    let mut current_modified = String::new();

    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            // 空行 = 条目分隔
            if !current_path.is_empty() {
                entries.push(ListEntry {
                    path: std::mem::take(&mut current_path),
                    size: current_size,
                    compressed_size: current_compressed,
                    is_directory: current_is_dir,
                    modified: std::mem::take(&mut current_modified),
                });
                current_size = 0;
                current_compressed = 0;
                current_is_dir = false;
            }
            continue;
        }

        // 跳过 header/footer
        if trimmed.starts_with("--") || trimmed.starts_with("7z") || trimmed.starts_with("Listing") {
            continue;
        }

        // 解析 Key = Value
        if let Some(eq) = trimmed.find('=') {
            let key = trimmed[..eq].trim();
            let val = trimmed[eq + 1..].trim();
            match key {
                "Path" => current_path = val.to_string(),
                "Size" => current_size = val.parse().unwrap_or(0),
                "Compressed" | "Compressed Size" => current_compressed = val.parse().unwrap_or(0),
                "Folder" => current_is_dir = val == "+" || val == "1",
                "Modified" => current_modified = val.to_string(),
                _ => {}
            }
        }
    }

    // 最后一个条目（如果没有尾随空行）
    if !current_path.is_empty() {
        entries.push(ListEntry {
            path: current_path,
            size: current_size,
            compressed_size: current_compressed,
            is_directory: current_is_dir,
            modified: current_modified,
        });
    }

    Ok(entries)
}

// ── 进度解析 ──────────────────────────────

/// 解析 7zz -bsp1 进度行，返回百分比 (0.0–100.0)
///
/// -bsp1 输出示例：
///   "  0%     - filename.7z"
///   " 45% 34 - filename.7z"
///   " 89% 67 - filename.7z"
///   "100% 75 - filename.7z"
///   "Everything is Ok"
fn parse_bsp1_progress(line: &str) -> Option<f64> {
    let trimmed = line.trim();
    // 跳过非进度行
    if trimmed.is_empty() || trimmed.starts_with("Everything") || trimmed.starts_with("Error") {
        return None;
    }

    // 找最后一个 "%" — 7zz 用 \b 覆写同一行，line 里可能有多个百分比
    let pct_pos = trimmed.rfind('%')?;
    let num_part = trimmed[..pct_pos].trim();
    if num_part.is_empty() {
        return None;
    }
    num_part.parse::<f64>().ok()
}

/// 从 -bsp1 输出行中提取文件名
fn extract_filename_from_bsp1(line: &str) -> Option<String> {
    // 格式: " 45% 34 - filename"
    if let Some(dash_pos) = line.find(" - ") {
        let fname = line[dash_pos + 3..].trim();
        if !fname.is_empty() {
            return Some(fname.to_string());
        }
    }
    None
}

// ── 工具函数 ──────────────────────────────

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

fn random_hex(len: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{:0width$x}", t % 10u128.pow(len as u32), width = len)
}

// ═══════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bsp1_zero() {
        assert_eq!(parse_bsp1_progress("  0%     - file.7z"), Some(0.0));
    }

    #[test]
    fn test_parse_bsp1_mid() {
        assert_eq!(parse_bsp1_progress(" 45% 34 - file.7z"), Some(45.0));
    }

    #[test]
    fn test_parse_bsp1_hundred() {
        assert_eq!(parse_bsp1_progress("100% 75 - file.7z"), Some(100.0));
    }

    #[test]
    fn test_parse_bsp1_non_progress() {
        assert_eq!(parse_bsp1_progress("Everything is Ok"), None);
        assert_eq!(parse_bsp1_progress(""), None);
        assert_eq!(parse_bsp1_progress("Error: something"), None);
    }

    #[test]
    fn test_parse_bsp1_filename() {
        assert_eq!(
            extract_filename_from_bsp1(" 45% 34 - dir/subdir/file.bin"),
            Some("dir/subdir/file.bin".to_string())
        );
        assert_eq!(
            extract_filename_from_bsp1("  0%     - archive.7z"),
            Some("archive.7z".to_string())
        );
        assert_eq!(extract_filename_from_bsp1("Everything is Ok"), None);
    }

    #[test]
    fn test_json_escape() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape(r#"say "hi""#), r#"say \"hi\""#);
        assert_eq!(json_escape("a\nb"), "a\\nb");
    }

    #[test]
    fn test_parse_slt_basic() {
        let input = r#"7z 21.07 : ...
--
Path = dir/file1.txt
Size = 1234
Compressed = 567
Folder =
Modified = 2024-01-01
--
Path = subdir
Size = 0
Compressed = 0
Folder = +
Modified = 2024-01-01
"#;
        let entries = parse_slt_output(input).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "dir/file1.txt");
        assert_eq!(entries[0].size, 1234);
        assert!(!entries[0].is_directory);
        assert_eq!(entries[1].path, "subdir");
        assert!(entries[1].is_directory);
    }
}
