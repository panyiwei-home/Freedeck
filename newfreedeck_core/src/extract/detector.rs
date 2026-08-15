//! 归档检测器 — 魔数识别 + 分卷主卷定位 + 目录扫描
//!
//! 参考：
//!   - 7-Zip 官方分卷命名约定
//!   - sevenz-rust (https://github.com/dyura/sevenz-rust) 格式检测模式
//!
//! 零外部依赖 — 纯 std::fs + 手动字节匹配

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

// ── 归档格式枚举 ──────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    SevenZip,  // .7z
    Zip,       // .zip
    Rar,       // .rar
    Tar,       // .tar
    Gzip,      // .gz / .tgz
    Bzip2,     // .bz2
    Xz,        // .xz
    Unknown,
}

impl ArchiveFormat {
    /// 返回格式的标准扩展名（不含点）
    pub fn extension(&self) -> &str {
        match self {
            ArchiveFormat::SevenZip => "7z",
            ArchiveFormat::Zip => "zip",
            ArchiveFormat::Rar => "rar",
            ArchiveFormat::Tar => "tar",
            ArchiveFormat::Gzip => "gz",
            ArchiveFormat::Bzip2 => "bz2",
            ArchiveFormat::Xz => "xz",
            ArchiveFormat::Unknown => "",
        }
    }

    /// 从扩展名猜测格式（不读文件内容）
    pub fn from_extension(name: &str) -> Self {
        let n = name.to_lowercase();
        if n.ends_with(".7z") { return ArchiveFormat::SevenZip; }
        if n.ends_with(".zip") || n.ends_with(".jar") || n.ends_with(".apk") { return ArchiveFormat::Zip; }
        if n.ends_with(".rar") { return ArchiveFormat::Rar; }
        if n.ends_with(".tar") { return ArchiveFormat::Tar; }
        if n.ends_with(".gz") || n.ends_with(".tgz") { return ArchiveFormat::Gzip; }
        if n.ends_with(".bz2") || n.ends_with(".tbz2") { return ArchiveFormat::Bzip2; }
        if n.ends_with(".xz") || n.ends_with(".txz") { return ArchiveFormat::Xz; }
        ArchiveFormat::Unknown
    }
}

// ── 魔数检测 ──────────────────────────────

/// 读取文件头 magic bytes 判断真实归档格式。
/// 不依赖扩展名，防止 `.exe` 改名为 `.7z` 误判。
pub fn detect_format_by_magic(path: &Path) -> ArchiveFormat {
    let mut file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return ArchiveFormat::Unknown,
    };
    let mut buf = [0u8; 16];
    let n = file.read(&mut buf).unwrap_or(0);
    if n < 4 {
        return ArchiveFormat::Unknown;
    }

    // 7z: 37 7A BC AF 27 1C
    if n >= 6 && buf[0] == 0x37 && buf[1] == 0x7A && buf[2] == 0xBC && buf[3] == 0xAF && buf[4] == 0x27 && buf[5] == 0x1C {
        return ArchiveFormat::SevenZip;
    }

    // ZIP: 50 4B 03 04 (PK..) or 50 4B 05 06 (empty) or 50 4B 07 08 (spanned)
    if buf[0] == 0x50 && buf[1] == 0x4B && (buf[2] == 0x03 || buf[2] == 0x05 || buf[2] == 0x07) {
        return ArchiveFormat::Zip;
    }

    // RAR v5: 52 61 72 21 1A 07 01 00
    if n >= 8 && buf[0] == 0x52 && buf[1] == 0x61 && buf[2] == 0x72 && buf[3] == 0x21 && buf[4] == 0x1A && buf[5] == 0x07 {
        return ArchiveFormat::Rar;
    }

    // RAR v4: 52 61 72 21 1A 07 00
    if n >= 7 && buf[0] == 0x52 && buf[1] == 0x61 && buf[2] == 0x72 && buf[3] == 0x21 && buf[4] == 0x1A && buf[5] == 0x07 && buf[6] == 0x00 {
        return ArchiveFormat::Rar;
    }

    // Gzip: 1F 8B
    if buf[0] == 0x1F && buf[1] == 0x8B {
        return ArchiveFormat::Gzip;
    }

    // Bzip2: 42 5A 68 (BZh)
    if buf[0] == 0x42 && buf[1] == 0x5A && buf[2] == 0x68 {
        return ArchiveFormat::Bzip2;
    }

    // XZ: FD 37 7A 58 5A 00
    if n >= 6 && buf[0] == 0xFD && buf[1] == 0x37 && buf[2] == 0x7A && buf[3] == 0x58 && buf[4] == 0x5A && buf[5] == 0x00 {
        return ArchiveFormat::Xz;
    }

    // tar: ustar magic at offset 257, 但这里只做快速检测
    // 如果扩展名是 .tar 且不是上述格式，按 tar 处理
    // (完整 tar 检测要读 offset 257，这里做轻量版：检查是否全为可打印 ascii)

    ArchiveFormat::Unknown
}

/// 综合检测：优先魔数，回退扩展名
pub fn detect_format(path: &Path) -> ArchiveFormat {
    let magic = detect_format_by_magic(path);
    if magic != ArchiveFormat::Unknown {
        return magic;
    }
    // 回退扩展名
    let name = path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    ArchiveFormat::from_extension(name)
}

// ── 分卷信息 ──────────────────────────────

/// 分卷检测结果
#[derive(Debug, Clone)]
pub struct SplitVolumeInfo {
    /// 去除所有分卷后缀后的基础名（例如 "game" 或 "game.7z"）
    pub base_name: String,
    /// 分卷序号（从 0 或 1 开始）
    pub volume_index: usize,
    /// 分卷总数（如果可推断）
    pub volume_count: Option<usize>,
}

/// 检测文件名是否匹配分卷模式。
///
/// 支持的命名模式：
///   - `.partN.rar` / `.partNN.rar`  → RAR 新式分卷
///   - `.partN.7z` / `.partNN.7z`    → 7z 分卷
///   - `.7z.NNN` / `.zip.NNN`        → 数字后缀分卷
///   - `.rNN` / `.zNN`               → RAR/ZIP 旧式分卷
///   - `.NNN`                        → 纯数字后缀（需 ≥ 001）
pub fn parse_volume_name(filename: &str) -> Option<SplitVolumeInfo> {
    let n = filename.to_lowercase();
    let original = filename;

    // ── 规则 1: .partN.ext  (part1.rar, part01.rar, part1.7z)
    for ext in &["rar", "7z", "zip", "exe"] {
        let suffix = format!(".{}", ext);
        if n.ends_with(&suffix) {
            let before = &n[..n.len() - suffix.len()];
            // 找 ".part"
            if let Some(part_pos) = before.rfind(".part") {
                let num_str = &before[part_pos + 5..]; // after ".part"
                if !num_str.is_empty() && num_str.chars().all(|c| c.is_ascii_digit()) {
                    if let Ok(idx) = num_str.parse::<usize>() {
                        let base = &original[..part_pos];
                        return Some(SplitVolumeInfo {
                            base_name: base.to_string(),
                            volume_index: idx,
                            volume_count: None,
                        });
                    }
                }
            }
        }
    }

    // ── 规则 2: .ext.NNN (7z.001, zip.001, rar.001)
    for ext in &["7z", "zip", "rar", "iso", "gz", "bz2", "xz", "tar"] {
        let suffix = format!(".{}", ext);
        if n.ends_with(&suffix) {
            // 在 ext 之后还要有 .NNN
            // 实际上 "name.7z.001" → n.ends_with("7z") 是 false，因为实际结尾是 "001"
            // 需要在完整文件名中查找
        }
    }
    // 正确的做法：反向查找 ".NNN" 后缀
    // 检查最后一段是否是纯数字
    if let Some(last_dot) = n.rfind('.') {
        let after = &n[last_dot + 1..];
        if after.len() >= 2 && after.len() <= 4 && after.chars().all(|c| c.is_ascii_digit()) {
            let idx: usize = after.parse().unwrap_or(usize::MAX);
            if idx > 9999 { return None; } // 不合理的大序号
            let before = &original[..last_dot];

            // 检查 before 是否以已知扩展名结尾
            for ext in &["7z", "zip", "rar", "iso", "gz", "bz2", "xz", "tar", "001", "002", "003"] {
                let ext_dot = format!(".{}", ext);
                if before.to_lowercase().ends_with(&ext_dot) {
                    let base = &before[..before.len() - ext_dot.len()];
                    return Some(SplitVolumeInfo {
                        base_name: base.to_string(),
                        volume_index: idx,
                        volume_count: None,
                    });
                }
            }

            // 没有已知扩展名，但数字后缀 ≥ 001 且 ≤ 999
            // 需要额外启发式：排除版本号 .v001
            if idx >= 1 && idx <= 999 {
                let before_lower = before.to_lowercase();
                // 排除 .vNNN 版本号模式
                if before_lower.ends_with(".v") {
                    return None;
                }
                // 排除 .rNN 但 .r 后不是数字结尾的情况
                // (已经在上面的循环中处理)

                return Some(SplitVolumeInfo {
                    base_name: before.to_string(),
                    volume_index: idx,
                    volume_count: None,
                });
            }
        }
    }

    // ── 规则 3: .rNN, .zNN (RAR/ZIP 旧式分卷，如 .r00, .r01, .z01)
    if n.len() > 4 {
        let last4 = &n[n.len() - 4..]; // e.g. ".r00"
        if last4.len() == 4 {
            let ch = last4.as_bytes()[1]; // 'r' or 'z'
            let num = &last4[2..]; // "00"
            if (ch == b'r' || ch == b'z') && last4.starts_with('.') && num.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(idx) = num.parse::<usize>() {
                    if idx < 100 {
                        let base = &original[..n.len() - 4];
                        return Some(SplitVolumeInfo {
                            base_name: base.to_string(),
                            volume_index: idx,
                            volume_count: None,
                        });
                    }
                }
            }
        }
    }

    // ── 规则 4: WinRAR 旧式 .000, .001 等（第一个卷是 .rar）
    // 已在规则 2 中覆盖

    None
}

// ── 主卷定位 ──────────────────────────────

/// 完整的归档信息
#[derive(Debug, Clone)]
pub struct ArchiveInfo {
    /// 归档格式
    pub format: ArchiveFormat,
    /// 是否分卷
    pub is_split: bool,
    /// 主卷 / 首卷路径（解压时传给 7zz 的路径）
    pub main_volume: PathBuf,
    /// 所有分卷路径（按序号升序）
    pub all_volumes: Vec<PathBuf>,
    /// 分卷数（0 = 非分卷）
    pub volume_count: usize,
    /// 归档内文件数（需 list 才能获取，初始为 0）
    pub file_count: u64,
    /// 压缩后总大小（字节）
    pub compressed_size: u64,
}

/// 定位主卷并扫描同目录下所有分卷。
///
/// 对于分卷归档，7zz 只需要传入主卷/首卷路径即可自动识别其余分卷。
/// 主卷定位逻辑：
///   - `.partN.rar`  → 找 `.part1.rar`（或序号最小的 `.partN.rar`）
///   - `.7z.NNN`     → 找 `.7z.001`
///   - `.rNN`        → 找 `.rar` 同 base 文件
///   - `.zNN`        → 找 `.zip` 同 base 文件
///   - `.NNN`        → 找 `.001`
pub fn detect_archive(input_path: &Path) -> Option<ArchiveInfo> {
    let input_path = input_path.canonicalize().unwrap_or_else(|_| input_path.to_path_buf());
    if !input_path.exists() {
        return None;
    }

    let format = detect_format(&input_path);
    let filename = input_path.file_name()?.to_str()?;

    // 尝试解析分卷名
    let parent = input_path.parent().unwrap_or_else(|| Path::new("."));

    if let Some(vol_info) = parse_volume_name(filename) {
        // ── 这是分卷 ──
        let base = vol_info.base_name;

        // 扫描同目录下所有同 base 的分卷文件
        let mut volumes: Vec<PathBuf> = Vec::new();
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(info) = parse_volume_name(&name_str) {
                    if info.base_name == base {
                        volumes.push(entry.path());
                    }
                }
                // 同时检查旧式 RAR 分卷的 .rar 主文件
                if name_str.to_lowercase() == format!("{}.rar", base).to_lowercase()
                    || name_str.to_lowercase() == format!("{}.zip", base).to_lowercase()
                {
                    volumes.push(entry.path());
                }
            }
        }

        // 按序号排序
        volumes.sort_by_key(|p| {
            let n = p.file_name().unwrap_or_default().to_string_lossy().to_string();
            parse_volume_name(&n).map(|v| v.volume_index).unwrap_or(0)
        });
        volumes.dedup();

        let volume_count = volumes.len();
        let mut total_size: u64 = 0;
        for v in &volumes {
            if let Ok(meta) = fs::metadata(v) {
                total_size += meta.len();
            }
        }

        // 主卷 = 序号最小的那个（通常 .001 / .part1 / .rar）
        let main_volume = volumes.first().cloned().unwrap_or_else(|| input_path.clone());

        return Some(ArchiveInfo {
            format,
            is_split: volume_count > 1,
            main_volume,
            all_volumes: volumes,
            volume_count,
            file_count: 0,
            compressed_size: total_size,
        });
    }

    // ── 非分卷，但检查是否有隐藏的分卷伙伴 ──
    // 例如：game.rar 旁边有 game.r00, game.r01
    let base_no_ext = strip_extension(filename);
    let mut companion_volumes: Vec<PathBuf> = vec![input_path.clone()];

    if let Ok(entries) = fs::read_dir(parent) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(info) = parse_volume_name(&name_str) {
                if info.base_name == base_no_ext {
                    companion_volumes.push(entry.path());
                }
            }
        }
    }

    companion_volumes.sort_by_key(|p| {
        let n = p.file_name().unwrap_or_default().to_string_lossy().to_string();
        parse_volume_name(&n).map(|v| v.volume_index).unwrap_or(0)
    });
    companion_volumes.dedup();

    let volume_count = companion_volumes.len();
    let is_split = volume_count > 1;
    let mut total_size: u64 = 0;
    for v in &companion_volumes {
        if let Ok(meta) = fs::metadata(v) {
            total_size += meta.len();
        }
    }

    Some(ArchiveInfo {
        format,
        is_split,
        main_volume: input_path.clone(),
        all_volumes: companion_volumes,
        volume_count,
        file_count: 0,
        compressed_size: total_size,
    })
}

/// 去掉文件名最后一个扩展名
fn strip_extension(filename: &str) -> String {
    if let Some(pos) = filename.rfind('.') {
        filename[..pos].to_string()
    } else {
        filename.to_string()
    }
}

// ═══════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── 魔数检测 ──────────────────────────

    #[test]
    fn test_detect_7z_magic() {
        // 7z magic: 37 7A BC AF 27 1C
        let data = [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x01];
        let tmp = std::env::temp_dir().join("test_magic.7z");
        std::fs::write(&tmp, &data).ok();
        let fmt = detect_format_by_magic(&tmp);
        assert_eq!(fmt, ArchiveFormat::SevenZip);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_detect_zip_magic() {
        let data = [0x50, 0x4B, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00];
        let tmp = std::env::temp_dir().join("test_magic.zip");
        std::fs::write(&tmp, &data).ok();
        let fmt = detect_format_by_magic(&tmp);
        assert_eq!(fmt, ArchiveFormat::Zip);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_detect_rar5_magic() {
        let data = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];
        let tmp = std::env::temp_dir().join("test_magic.rar");
        std::fs::write(&tmp, &data).ok();
        let fmt = detect_format_by_magic(&tmp);
        assert_eq!(fmt, ArchiveFormat::Rar);
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_detect_unknown() {
        let data = [0x00, 0x00, 0x00, 0x00];
        let tmp = std::env::temp_dir().join("test_magic.bin");
        std::fs::write(&tmp, &data).ok();
        let fmt = detect_format_by_magic(&tmp);
        assert_eq!(fmt, ArchiveFormat::Unknown);
        let _ = std::fs::remove_file(&tmp);
    }

    // ── 扩展名检测 ────────────────────────

    #[test]
    fn test_format_from_extension() {
        assert_eq!(ArchiveFormat::from_extension("game.7z"), ArchiveFormat::SevenZip);
        assert_eq!(ArchiveFormat::from_extension("game.zip"), ArchiveFormat::Zip);
        assert_eq!(ArchiveFormat::from_extension("game.rar"), ArchiveFormat::Rar);
        assert_eq!(ArchiveFormat::from_extension("game.tar.gz"), ArchiveFormat::Gzip);
        assert_eq!(ArchiveFormat::from_extension("game.tar.bz2"), ArchiveFormat::Bzip2);
        assert_eq!(ArchiveFormat::from_extension("game.tar.xz"), ArchiveFormat::Xz);
        assert_eq!(ArchiveFormat::from_extension("readme.txt"), ArchiveFormat::Unknown);
    }

    // ── 分卷名解析 ─────────────────────────

    #[test]
    fn test_parse_part1_rar() {
        let info = parse_volume_name("game.part1.rar").unwrap();
        assert_eq!(info.base_name, "game");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_part01_rar() {
        let info = parse_volume_name("game.part01.rar").unwrap();
        assert_eq!(info.base_name, "game");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_part2_rar() {
        let info = parse_volume_name("game.part2.rar").unwrap();
        assert_eq!(info.base_name, "game");
        assert_eq!(info.volume_index, 2);
    }

    #[test]
    fn test_parse_7z_001() {
        let info = parse_volume_name("rom.7z.001").unwrap();
        assert_eq!(info.base_name, "rom");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_7z_002() {
        let info = parse_volume_name("rom.7z.002").unwrap();
        assert_eq!(info.base_name, "rom");
        assert_eq!(info.volume_index, 2);
    }

    #[test]
    fn test_parse_zip_001() {
        let info = parse_volume_name("data.zip.001").unwrap();
        assert_eq!(info.base_name, "data");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_r00() {
        let info = parse_volume_name("disk.r00").unwrap();
        assert_eq!(info.base_name, "disk");
        assert_eq!(info.volume_index, 0);
    }

    #[test]
    fn test_parse_r01() {
        let info = parse_volume_name("disk.r01").unwrap();
        assert_eq!(info.base_name, "disk");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_z01() {
        let info = parse_volume_name("archive.z01").unwrap();
        assert_eq!(info.base_name, "archive");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_generic_001() {
        let info = parse_volume_name("backup.001").unwrap();
        assert_eq!(info.base_name, "backup");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_generic_003() {
        let info = parse_volume_name("backup.003").unwrap();
        assert_eq!(info.base_name, "backup");
        assert_eq!(info.volume_index, 3);
    }

    #[test]
    fn test_not_volume_regular_file() {
        assert!(parse_volume_name("game.7z").is_none());
        assert!(parse_volume_name("game.zip").is_none());
        assert!(parse_volume_name("game.rar").is_none());
        assert!(parse_volume_name("readme.txt").is_none());
    }

    #[test]
    fn test_not_volume_version() {
        // .v001 是版本号，不是分卷
        assert!(parse_volume_name("movie.v001").is_none());
        assert!(parse_volume_name("app.v2").is_none());
    }

    #[test]
    fn test_parse_rar_iso_001() {
        let info = parse_volume_name("image.iso.001").unwrap();
        assert_eq!(info.base_name, "image");
        assert_eq!(info.volume_index, 1);
    }

    #[test]
    fn test_parse_tar_gz_001() {
        // .tar.gz.001 不太常见但技术上可行
        let info = parse_volume_name("archive.tar.gz.001").unwrap();
        // base_name = "archive.tar" (去掉了 .gz.001)
        assert_eq!(info.volume_index, 1);
    }

    // ── 主卷定位 ───────────────────────────
    // (需要实际文件系统，在集成测试中验证)
}
