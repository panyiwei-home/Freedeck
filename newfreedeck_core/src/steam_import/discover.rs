//! exe 发现引擎 — 从解压目录中匹配游戏可执行文件
//!
//! 策略：
//!   1. openpath 精确匹配（优先）
//!   2. 启发式扫描（回退）

use std::fs;
use std::path::{Path, PathBuf};

/// 排除列表：已知非游戏主程序的 exe 文件名
const EXCLUDE_NAMES: &[&str] = &[
    "vc_redist", "vcredist", "unitycrashhandler",
    "unins", "uninst", "uninstall",
    "dxsetup", "dotnet", "dotnetfx",
];

/// 发现结果
#[derive(Debug, Clone)]
pub struct DiscoverResult {
    /// 最终选中的 exe 路径
    pub exe_path: String,
    /// 所有候选 exe（按分数降序）
    pub candidates: Vec<String>,
    /// 匹配方式: "openpath" | "heuristic"
    pub method: String,
    /// 是否找到
    pub found: bool,
}

impl DiscoverResult {
    pub fn not_found() -> Self {
        DiscoverResult {
            exe_path: String::new(),
            candidates: Vec::new(),
            method: "none".into(),
            found: false,
        }
    }
}

/// 从解压目录发现 exe
///
/// - `extract_dir`: 解压根目录
/// - `openpath`: 远程目录中的 openpath 字段（可为空）
pub fn discover_exe(extract_dir: &Path, openpath: &str) -> DiscoverResult {
    // ── 策略 1: openpath 精确匹配 ──
    if !openpath.is_empty() {
        // openpath 可能是 "game.exe" 或 "Binaries/Win64/game.exe"
        let full_path = extract_dir.join(openpath);
        if full_path.exists() && full_path.is_file() {
            return DiscoverResult {
                exe_path: full_path.to_string_lossy().to_string(),
                candidates: vec![full_path.to_string_lossy().to_string()],
                method: "openpath".into(),
                found: true,
            };
        }

        // openpath 可能只是文件名，尝试在解压目录中搜索
        let filename = Path::new(openpath)
            .file_name()
            .map(|n| n.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        if !filename.is_empty() {
            let mut found = Vec::new();
            find_exe_by_name(extract_dir, &filename, &mut found);
            if !found.is_empty() {
                let best = found[0].clone();
                return DiscoverResult {
                    exe_path: best.clone(),
                    candidates: found,
                    method: "openpath_basename".into(),
                    found: true,
                };
            }
        }
    }

    // ── 策略 2: 启发式扫描 ──
    let candidates = scan_exes(extract_dir);
    if candidates.is_empty() {
        return DiscoverResult::not_found();
    }

    if candidates.len() == 1 {
        return DiscoverResult {
            exe_path: candidates[0].clone(),
            candidates,
            method: "heuristic_single".into(),
            found: true,
        };
    }

    // 评分排序
    let dir_name = extract_dir
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let mut scored: Vec<(String, i32)> = candidates
        .iter()
        .map(|p| {
            let score = score_exe(p, extract_dir, &dir_name);
            (p.clone(), score)
        })
        .collect();
    scored.sort_by(|a, b| b.1.cmp(&a.1));

    // 过滤：分数太低的不要
    let best = if scored[0].1 >= 20 {
        &scored[0].0
    } else {
        return DiscoverResult {
            exe_path: String::new(),
            candidates: scored.iter().map(|s| s.0.clone()).collect(),
            method: "heuristic_low_score".into(),
            found: false,
        };
    };

    DiscoverResult {
        exe_path: best.clone(),
        candidates: scored.iter().map(|s| s.0.clone()).collect(),
        method: "heuristic".into(),
        found: true,
    }
}

/// 按文件名搜索 exe（大小写不敏感）
fn find_exe_by_name(dir: &Path, name_lower: &str, results: &mut Vec<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let file_name_lower = path
                .file_name()
                .map(|n| n.to_string_lossy().to_lowercase())
                .unwrap_or_default();

            if path.is_dir() {
                find_exe_by_name(&path, name_lower, results);
            } else if file_name_lower.ends_with(".exe") && file_name_lower == name_lower {
                results.push(path.to_string_lossy().to_string());
            }
        }
    }
}

/// 递归扫描所有 .exe 文件，排除已知非主程序
fn scan_exes(dir: &Path) -> Vec<String> {
    let mut result = Vec::new();
    scan_exes_recursive(dir, &mut result);
    result
}

fn scan_exes_recursive(dir: &Path, result: &mut Vec<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan_exes_recursive(&path, result);
            } else if let Some(ext) = path.extension() {
                if ext.to_ascii_lowercase() == "exe" {
                    let name_lower = path
                        .file_stem()
                        .map(|n| n.to_string_lossy().to_lowercase())
                        .unwrap_or_default();

                    // 排除已知非主程序
                    let excluded = EXCLUDE_NAMES.iter().any(|ex| name_lower.contains(ex));
                    if !excluded {
                        result.push(path.to_string_lossy().to_string());
                    }
                }
            }
        }
    }
}

/// 对 exe 候选评分（越高越好）
fn score_exe(path: &str, root: &Path, dir_name: &str) -> i32 {
    let p = Path::new(path);
    let file_stem = p
        .file_stem()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();

    let mut score = 0i32;

    // 文件名与目录名相似（Levenshtein < 4 或者包含关系）
    if file_stem.contains(dir_name) || dir_name.contains(&file_stem) {
        score += 30;
    } else if levenshtein_limit(&file_stem, dir_name, 3) {
        score += 25;
    }

    // 路径深度
    if let Ok(relative) = p.strip_prefix(root) {
        let depth = relative.components().count();
        match depth {
            1 => score += 20,  // 根目录
            2 => score += 15,  // 一层子目录
            3 => score += 5,   // 两层
            _ => score -= 10,   // 三层及以上
        }
    }

    // 文件大小
    if let Ok(meta) = fs::metadata(p) {
        let size = meta.len();
        if size > 10_000_000 {
            score += 10; // >10MB
        } else if size > 1_000_000 {
            score += 8;  // >1MB
        } else if size > 100_000 {
            score += 3;  // >100KB
        }
    }

    score
}

/// 简化 Levenshtein 距离（限制最大距离，超过即返回 false）
fn levenshtein_limit(a: &str, b: &str, limit: usize) -> bool {
    let a_len = a.chars().count();
    let b_len = b.chars().count();

    if (a_len as isize - b_len as isize).unsigned_abs() > limit {
        return false;
    }

    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();

    let mut prev = (0..=b_len).collect::<Vec<usize>>();
    let mut curr = vec![0usize; b_len + 1];

    for i in 1..=a_len {
        curr[0] = i;
        let mut min_row = usize::MAX;
        for j in 1..=b_len {
            let cost = if a_chars[i - 1] == b_chars[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1)
                .min(curr[j - 1] + 1)
                .min(prev[j - 1] + cost);
            min_row = min_row.min(curr[j]);
        }
        if min_row > limit {
            return false;
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_len] <= limit
}

// ═══════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein() {
        assert!(levenshtein_limit("game", "game", 0));
        assert!(levenshtein_limit("game", "gamr", 1));
        assert!(!levenshtein_limit("game", "gxxx", 1));
        assert!(levenshtein_limit("Duel Corp", "duelcorp", 3));
    }

    #[test]
    fn test_no_dir() {
        let r = discover_exe(Path::new("/nonexistent_dir_12345"), "");
        assert!(!r.found);
    }
}
