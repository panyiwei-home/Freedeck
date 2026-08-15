//! aria2 JSON-RPC 客户端 + 子进程管理 — 零外部依赖
//! HTTP 用 std::net::TcpStream 手写实现

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// ── 工具 ──────────────────────────────────

fn pick_free_port() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| e.to_string())?
        .local_addr()
        .map_err(|e| e.to_string())
        .map(|a| a.port())
}

fn random_hex(len: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{:0width$x}", t, width = len)
}

/// JSON 字符串转义
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

// ── 迷你 JSON 字段提取 ──────────────────

fn json_find_val<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{}\"", key);
    let pos = json.find(&pat)?;
    let after = &json[pos + pat.len()..];
    let colon = after.find(':')?;
    let val = after[colon + 1..].trim_start();
    if val.starts_with('"') {
        let end = val[1..].find('"')?;
        Some(&val[1..=end])
    } else if val.starts_with("null") {
        None
    } else {
        let end = val.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-').unwrap_or(val.len());
        if end == 0 { None } else { Some(&val[..end]) }
    }
}

fn json_get_str(json: &str, key: &str) -> Option<String> {
    json_find_val(json, key).map(|s| s.to_string())
}

fn json_get_u64(json: &str, key: &str) -> Option<u64> {
    json_find_val(json, key)?.parse().ok()
}

// ── HTTP + JSON-RPC ───────────────────────

/// 发送 JSON-RPC 请求，返回响应 body 字符串
fn rpc_call(host: &str, port: u16, body: &str) -> Result<String, String> {
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| format!("addr parse {}:{}: {}", host, port, e))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|e| format!("connect {}:{}: {}", host, port, e))?;

    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let request = format!(
        "POST /jsonrpc HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        host, port, body.len(), body
    );
    stream.write_all(request.as_bytes()).map_err(|e| format!("write: {}", e))?;

    // 读完整响应
    let mut buf = [0u8; 4096];
    let mut all = Vec::new();
    loop {
        let n = stream.read(&mut buf).map_err(|e| format!("read: {}", e))?;
        if n == 0 {
            break;
        }
        all.extend_from_slice(&buf[..n]);
        if all.len() > 512 * 1024 {
            return Err("response too large".into());
        }
    }

    let resp = String::from_utf8_lossy(&all);
    let body_start = resp.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
    let body = resp[body_start..].trim();

    // 检查 HTTP 错误
    if body.is_empty() {
        return Err("empty response".into());
    }

    // 检查 JSON-RPC error
    if body.contains("\"error\"") {
        let msg = json_get_str(body, "message").unwrap_or_else(|| "unknown error".into());
        return Err(format!("aria2 RPC: {}", msg));
    }

    Ok(body.to_string())
}

// ── Aria2Rpc ──────────────────────────────

pub struct Aria2Rpc {
    port: u16,
    secret: String,
    counter: u64,
}

impl Aria2Rpc {
    pub fn new(port: u16, secret: &str) -> Self {
        Self { port, secret: secret.to_string(), counter: 0 }
    }

    fn next_id(&mut self) -> String {
        self.counter += 1;
        self.counter.to_string()
    }

    /// 低级 JSON-RPC 调用
    fn call(&mut self, method: &str, params_json: &str) -> Result<String, String> {
        let body = if params_json.is_empty() {
            format!(
                r#"{{"jsonrpc":"2.0","id":"{}","method":"{}","params":["token:{}"]}}"#,
                self.next_id(), method, self.secret
            )
        } else {
            format!(
                r#"{{"jsonrpc":"2.0","id":"{}","method":"{}","params":["token:{}",{}]}}"#,
                self.next_id(), method, self.secret, params_json
            )
        };
        rpc_call("127.0.0.1", self.port, &body)
    }

    pub fn health(&mut self) -> Result<bool, String> {
        match self.call("aria2.getVersion", "") {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// 添加下载链接
    /// 注意：aria2.addUri 的第一个参数是 uris (JSON 数组)，不是单字符串
    pub fn add_uri(
        &mut self,
        uri: &str,
        dir_: &str,
        out: &str,
        cookie: &str,
    ) -> Result<String, String> {
        // 构建 header 数组
        let mut header_parts: Vec<String> = vec![
            r#""User-Agent: Mozilla/5.0 (Steam Deck)""#.into(),
            r#""Referer: https://cloud.189.cn/""#.into(),
        ];
        if !cookie.is_empty() {
            header_parts.push(format!(r#""Cookie: {}"#, json_escape(cookie)));
        }
        let headers_json = format!("[{}]", header_parts.join(","));

        // 注意：split、max-connection-per-server 必须是数字，continue 等必须是布尔
        let options = format!(
            r#"{{"dir":"{}","out":"{}","header":{},"split":16,"max-connection-per-server":16,"continue":true,"allow-overwrite":true,"auto-file-renaming":false}}"#,
            json_escape(dir_),
            json_escape(out),
            headers_json,
        );

        // uris 必须是 JSON 数组格式，即使只有一个 URL
        let uris_json = format!(r#"["{}"]"#, json_escape(uri));
        let params = format!("{},{}", uris_json, options);
        let resp = self.call("aria2.addUri", &params)
            .map_err(|e| format!("addUri RPC failed: {} (url={})", e, &uri[..uri.len().min(80)]))?;

        let gid = json_get_str(&resp, "result").unwrap_or_default();
        if gid.is_empty() || gid == "null" {
            Err("aria2.addUri returned empty gid".into())
        } else {
            Ok(gid)
        }
    }

    /// 查询下载状态
    pub fn tell_status(&mut self, gid: &str) -> Result<String, String> {
        let params = format!(
            r#""{}",["status","totalLength","completedLength","downloadSpeed","errorMessage","errorCode"]"#,
            gid
        );
        self.call("aria2.tellStatus", &params)
    }

    pub fn pause(&mut self, gid: &str) -> Result<(), String> {
        self.call("aria2.forcePause", &format!(r#""{}""#, gid))?;
        Ok(())
    }

    pub fn resume(&mut self, gid: &str) -> Result<(), String> {
        self.call("aria2.unpause", &format!(r#""{}""#, gid))?;
        Ok(())
    }

    pub fn remove(&mut self, gid: &str) -> Result<(), String> {
        let _ = self.call("aria2.remove", &format!(r#""{}""#, gid));
        let _ = self.call("aria2.removeDownloadResult", &format!(r#""{}""#, gid));
        Ok(())
    }
}

// ── 子进程管理 ────────────────────────────

pub fn spawn_aria2(work_dir: &str, binary_path: &str) -> Result<(Child, u16, String), String> {
    let port = pick_free_port()?;
    let secret = random_hex(32);
    let session_file = format!("{}/aria2.session", work_dir);
    let input_file = if std::path::Path::new(&session_file).exists() {
        session_file.clone()
    } else {
        "/dev/null".to_string()
    };

    let child = Command::new(binary_path)
        .args(&[
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            "--rpc-allow-origin-all=true",
            &format!("--rpc-listen-port={}", port),
            &format!("--rpc-secret={}", secret),
            &format!("--dir={}", work_dir),
            "--continue=true",
            "--max-concurrent-downloads=5",
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=1M",
            "--max-tries=0",
            "--retry-wait=5",
            "--connect-timeout=10",
            "--timeout=30",
            "--file-allocation=none",
            "--disk-cache=32M",
            "--check-integrity=true",
            "--always-resume=true",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--daemon=false",
            "--summary-interval=0",
            &format!("--input-file={}", input_file),
            &format!("--save-session={}", session_file),
            "--save-session-interval=15",
            &format!("--log={}/aria2.log", work_dir),
            "--log-level=notice",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .current_dir(work_dir)
        .spawn()
        .map_err(|e| format!("spawn aria2c: {} (path: {})", e, binary_path))?;

    Ok((child, port, secret))
}
