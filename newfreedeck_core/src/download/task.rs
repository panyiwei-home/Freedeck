//! 下载任务数据模型 — 对应方案 §2.1
//! 零外部依赖

use std::fmt;

/// 文件下载状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileState {
    Pending,
    Resolving,
    Queued,
    Downloading,
    Complete,
    Failed(String),
    Skipped,
}

impl fmt::Display for FileState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileState::Pending => write!(f, "Pending"),
            FileState::Resolving => write!(f, "Resolving"),
            FileState::Queued => write!(f, "Queued"),
            FileState::Downloading => write!(f, "Downloading"),
            FileState::Complete => write!(f, "Complete"),
            FileState::Failed(e) => write!(f, "Failed({})", e),
            FileState::Skipped => write!(f, "Skipped"),
        }
    }
}

/// 任务整体状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Created,
    Resolving,
    Downloading,
    Validating,
    Complete,
    Failed(String),
    Cancelled,
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Created => write!(f, "Created"),
            TaskState::Resolving => write!(f, "Resolving"),
            TaskState::Downloading => write!(f, "Downloading"),
            TaskState::Validating => write!(f, "Validating"),
            TaskState::Complete => write!(f, "Complete"),
            TaskState::Failed(e) => write!(f, "Failed({})", e),
            TaskState::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// 单个文件
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub file_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub is_directory: bool,
    pub download_url: Option<String>,
    pub gid: Option<String>,
    pub downloaded: u64,
    pub state: FileState,
    pub retry_count: u32,
}

/// 下载任务
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub task_id: String,
    pub files: Vec<FileEntry>,
    pub state: TaskState,
    pub created_at: u64,
}

impl FileEntry {
    pub fn new(file_id: &str, name: &str, size_bytes: u64) -> Self {
        Self {
            file_id: file_id.to_string(),
            name: name.to_string(),
            size_bytes,
            is_directory: false,
            download_url: None,
            gid: None,
            downloaded: 0,
            state: FileState::Pending,
            retry_count: 0,
        }
    }
}

impl DownloadTask {
    pub fn new(task_id: &str, files: Vec<FileEntry>) -> Self {
        Self { task_id: task_id.to_string(), files, state: TaskState::Created, created_at: 0 }
    }

    pub fn all_files_complete(&self) -> bool {
        self.files.iter().all(|f| matches!(f.state, FileState::Complete | FileState::Skipped))
    }

    pub fn any_file_failed(&self) -> bool {
        self.files.iter().any(|f| matches!(f.state, FileState::Failed(_)))
    }

    /// 序列化为 JSON（手动构建，不需要 serde）
    pub fn to_json(&self) -> String {
        let mut s = String::from("{");
        s.push_str(&format!("\"task_id\":\"{}\",", self.task_id));
        s.push_str(&format!("\"state\":\"{}\",", self.state));
        s.push_str("\"files\":[");
        for (i, f) in self.files.iter().enumerate() {
            if i > 0 { s.push(','); }
            s.push_str(&format!(
                "{{\"name\":\"{}\",\"size_bytes\":{},\"downloaded\":{},\"state\":\"{}\"}}",
                f.name, f.size_bytes, f.downloaded, f.state
            ));
        }
        s.push_str("]}");
        s
    }
}

/// 进度快照（用于 poll 返回）
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub task_id: String,
    pub task_state: String,
    pub files: Vec<FileProgress>,
}

#[derive(Debug, Clone)]
pub struct FileProgress {
    pub name: String,
    pub size_bytes: u64,
    pub downloaded: u64,
    pub speed: u64,
    pub state: String,
    pub error: String,
}

impl ProgressSnapshot {
    pub fn from_task(task: &DownloadTask) -> Self {
        Self {
            task_id: task.task_id.clone(),
            task_state: task.state.to_string(),
            files: task.files.iter().map(|f| FileProgress {
                name: f.name.clone(),
                size_bytes: f.size_bytes,
                downloaded: f.downloaded,
                speed: 0,
                state: f.state.to_string(),
                error: match &f.state { FileState::Failed(e) => e.clone(), _ => String::new() },
            }).collect(),
        }
    }

    pub fn to_json(&self) -> String {
        let mut s = String::from("{");
        s.push_str(&format!("\"task_id\":\"{}\",", self.task_id));
        s.push_str(&format!("\"task_state\":\"{}\",", self.task_state));
        s.push_str("\"files\":[");
        for (i, f) in self.files.iter().enumerate() {
            if i > 0 { s.push(','); }
            s.push_str(&format!(
                "{{\"name\":\"{}\",\"size_bytes\":{},\"downloaded\":{},\"speed\":{},\"state\":\"{}\",\"error\":\"{}\"}}",
                f.name, f.size_bytes, f.downloaded, f.speed, f.state, f.error
            ));
        }
        s.push_str("]}");
        s
    }
}
