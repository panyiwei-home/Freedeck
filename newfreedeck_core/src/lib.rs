//! newfreedeck_core — C-ABI 动态库
//!   - MD5 签名（手写内联，无依赖 crate）
//!   - CSV 查询（手写内联，无依赖 crate）
//!   - 下载引擎（tokio + reqwest + serde_json + regex + uuid）
//!   - 解压引擎（7zz 进程包装，魔数检测，分卷主卷定位）

mod download;
mod extract;
mod steam_import;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::OnceLock;

// ── 下载引擎全局单例 ──────────────────────

static ENGINE_READY: OnceLock<()> = OnceLock::new();
static mut PLUGIN_DIR: Option<String> = None;
static mut WORK_DIR: Option<String> = None;

fn engine_init() {
    unsafe {
        ENGINE_READY.get_or_init(|| {
            download::init_engine(
                PLUGIN_DIR.as_ref().unwrap(),
                WORK_DIR.as_ref().unwrap(),
            );
        });
    }
}

#[no_mangle]
pub extern "C" fn nfc_download_init(plugin_dir: *const c_char, work_dir: *const c_char) -> i32 {
    unsafe {
        PLUGIN_DIR = Some(cstr(plugin_dir).to_string());
        WORK_DIR = Some(cstr(work_dir).to_string());
    }
    engine_init();
    // 检查是否启动成功
    if std::path::Path::new(&format!(
        "{}/aria2.log",
        unsafe { WORK_DIR.as_ref().unwrap() }
    ))
    .exists()
    {
        0
    } else {
        1
    }
}

/// 启动下载：传入 JSON 数组 [{url, name, size}] 和 cookie
#[no_mangle]
pub extern "C" fn nfc_download_start(
    urls_json: *const c_char,
    names_json: *const c_char,
    sizes_json: *const c_char,
    cookie: *const c_char,
) -> *mut c_char {
    engine_init();
    let urls: Vec<String> = parse_json_str_array(unsafe { cstr(urls_json) });
    let names: Vec<String> = parse_json_str_array(unsafe { cstr(names_json) });
    let sizes: Vec<u64> = parse_json_u64_array(unsafe { cstr(sizes_json) });
    let cookie = unsafe { cstr(cookie) }.to_string();

    let eng = download::engine();
    match eng.start_download(urls, names, sizes, &cookie) {
        Ok(task_id) => cstring(&task_id),
        Err(e) => cstring(&format!("__err__{}", e)),
    }
}

/// 轮询下载进度 — 返回 JSON
#[no_mangle]
pub extern "C" fn nfc_download_poll(task_id: *const c_char) -> *mut c_char {
    engine_init();
    let tid = unsafe { cstr(task_id) }.to_string();
    let eng = download::engine();
    match eng.poll(&tid) {
        Ok(json) => cstring(&json),
        Err(e) => cstring(&format!("__err__{}", e)),
    }
}

/// 取消下载
#[no_mangle]
pub extern "C" fn nfc_download_cancel(task_id: *const c_char) -> i32 {
    engine_init();
    let tid = unsafe { cstr(task_id) }.to_string();
    let eng = download::engine();
    match eng.cancel(&tid) {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

// ═══════════════════════════════════════════
// MD5
// ═══════════════════════════════════════════

fn md5_hex(data: &[u8]) -> String {
    let mut state = [0x67452301u32, 0xefcdab89, 0x98badcfe, 0x10325476];
    let mut count = [0u32; 2];
    let mut buffer = [0u8; 64];
    let mut buf_pos = 0usize;

    const S: [u32; 64] = [
        7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
        5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
        4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
        6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21,
    ];
    const K: [u32; 64] = [
        0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,
        0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
        0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,
        0x6b901122,0xfd987193,0xa679438e,0x49b40821,
        0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,
        0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
        0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,
        0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
        0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,
        0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
        0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,
        0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
        0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,
        0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
        0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,
        0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391,
    ];

    fn transform(state: &mut [u32;4], block: &[u8;64]) {
        let mut x = [0u32;16];
        for i in 0..16 {
            x[i]=u32::from_le_bytes([block[i*4],block[i*4+1],block[i*4+2],block[i*4+3]]);
        }
        let mut a=state[0];let mut b=state[1];let mut c=state[2];let mut d=state[3];
        for i in 0..64 {
            let (f,g)=if i<16{((b&c)|(!b&d),i)}
            else if i<32{((d&b)|(!d&c),(5*i+1)%16)}
            else if i<48{(b^c^d,(3*i+5)%16)}
            else{(c^(b|!d),(7*i)%16)};
            let t=d;d=c;c=b;
            b=b.wrapping_add(a.wrapping_add(f).wrapping_add(K[i]).wrapping_add(x[g]).rotate_left(S[i]));
            a=t;
        }
        state[0]=state[0].wrapping_add(a);state[1]=state[1].wrapping_add(b);
        state[2]=state[2].wrapping_add(c);state[3]=state[3].wrapping_add(d);
    }

    for &byte in data {
        count[0]=count[0].wrapping_add(8);
        if count[0]<8{count[1]=count[1].wrapping_add(1);}
        buffer[buf_pos]=byte;buf_pos+=1;
        if buf_pos==64{transform(&mut state,&buffer);buf_pos=0;}
    }
    let mut pad=[0u8;128];
    let idx=buf_pos;pad[0]=0x80;
    let pad_len=if idx<56{56-idx}else{120-idx};
    let total_bits=(count[0] as u64)|((count[1] as u64)<<32);
    pad[pad_len..pad_len+8].copy_from_slice(&total_bits.to_le_bytes());
    let pad_data=&pad[..pad_len+8];
    let mut pos=0;
    let n1=pad_data.len().min(64-buf_pos);
    buffer[buf_pos..buf_pos+n1].copy_from_slice(&pad_data[..n1]);
    buf_pos+=n1;pos+=n1;
    if buf_pos==64{transform(&mut state,&buffer);buf_pos=0;}
    while pos<pad_data.len(){
        let n=(pad_data.len()-pos).min(64);
        buffer[..n].copy_from_slice(&pad_data[pos..pos+n]);
        if n==64{transform(&mut state,&buffer);}
        pos+=n;
    }
    let mut hex=String::with_capacity(32);
    for w in &state{for byte in &w.to_le_bytes(){
        hex.push(char::from_digit((byte>>4)as u32,16).unwrap_or('0'));
        hex.push(char::from_digit((byte&0xf)as u32,16).unwrap_or('0'));
    }}
    hex
}

// ═══════════════════════════════════════════
// CSV 查询
// ═══════════════════════════════════════════

static mut ITEMS_PTR: *const u8 = ptr::null();
static mut ITEMS_LEN: usize = 0;

const F_GAME_ID:usize=0;const F_TITLE:usize=1;const F_PLATFORM:usize=12;
const F_TITLE_ZH:usize=13;const F_TITLE_EN:usize=14;
const F_SIZE_BYTES:usize=20;const F_SIZE_TEXT:usize=21;const F_COVER_URL:usize=22;
const F_DOWN_URL:usize=6;const F_PWD:usize=8;

fn get_field(line:&str,idx:usize)->&str{
    let mut start=0;let mut in_q=false;let mut field=0;
    for(i,b)in line.as_bytes().iter().enumerate(){
        if in_q{if*b==b'"'{in_q=false;}}
        else if*b==b'"'{in_q=true;}
        else if*b==b','{
            if field==idx{
                let s=&line[start..i];
                if s.starts_with('"')&&s.ends_with('"')&&s.len()>=2{return &s[1..s.len()-1];}
                return s;
            }
            field+=1;start=i+1;
        }
    }
    let s=&line[start..];
    if s.starts_with('"')&&s.ends_with('"')&&s.len()>=2{&s[1..s.len()-1]}else{s}
}

fn match_line(line:&str,query:&str)->bool{
    if query.is_empty(){return true;}
    let q=query.to_lowercase();
    [F_TITLE,F_TITLE_ZH,F_TITLE_EN].iter().any(|&i|get_field(line,i).to_lowercase().contains(&q))
}

#[no_mangle]
pub extern "C" fn nfc_load(data:*const u8,len:usize){
    unsafe{ITEMS_PTR=data;ITEMS_LEN=len;}
}

#[no_mangle]
pub extern "C" fn nfc_query(page:u32,page_size:u32,query:*const c_char)->*mut c_char{
    let q=if query.is_null(){String::new()}else{unsafe{cstr(query)}.to_string()};
    let data=unsafe{
        if ITEMS_PTR.is_null(){return cstring("{\"items\":[],\"total\":0}");}
        std::str::from_utf8(std::slice::from_raw_parts(ITEMS_PTR,ITEMS_LEN)).unwrap_or("")
    };
    let mut matched:Vec<usize>=Vec::new();
    let mut off=0;
    while off<data.len(){
        let end=data[off..].find('\n').map(|p|off+p).unwrap_or(data.len());
        let line=data[off..end].trim_end_matches('\r');
        if off>0&&match_line(line,&q){matched.push(off);}
        off=end+1;
    }
    let total=matched.len();
    let p=(page.max(1)as usize).min(total);
    let ps=(page_size.max(1).min(100)as usize).min(total);
    let start=((p-1)*ps).min(total);
    let end=(start+ps).min(total);
    let has_more=end<total;
    let mut json=format!("{{\"items\":[");
    for(j,&off)in matched[start..end].iter().enumerate(){
        let e2=data[off..].find('\n').map(|p|off+p).unwrap_or(data.len());
        let line=data[off..e2].trim_end_matches('\r');
        if j>0{json.push(',');}
        json.push_str(&format!(
            "{{\"game_id\":\"{}\",\"title\":\"{}\",\"title_zh\":\"{}\",\"title_en\":\"{}\",\"platform\":\"{}\",\"cover_url\":\"{}\",\"down_url\":\"{}\",\"pwd\":\"{}\",\"size_text\":\"{}\",\"size_bytes\":{}}}",
            esc(get_field(line,F_GAME_ID)),esc(get_field(line,F_TITLE)),
            esc(get_field(line,F_TITLE_ZH)),esc(get_field(line,F_TITLE_EN)),
            esc(get_field(line,F_PLATFORM)),esc(get_field(line,F_COVER_URL)),
            esc(get_field(line,F_DOWN_URL)),esc(get_field(line,F_PWD)),
            esc(get_field(line,F_SIZE_TEXT)),get_field(line,F_SIZE_BYTES),
        ));
    }
    json.push_str(&format!("],\"page\":{},\"page_size\":{},\"total\":{},\"has_more\":{}}}",
        p,ps,total,has_more));
    cstring(&json)
}

fn esc(s:&str)->String{
    let mut r=String::with_capacity(s.len());
    for c in s.chars(){match c{
        '"'=>r.push_str("\\\""),'\\'=>r.push_str("\\\\"),
        '\n'=>r.push_str("\\n"),'\r'=>r.push_str("\\r"),'\t'=>r.push_str("\\t"),
        _=>r.push(c),
    }}
    r
}

// ═══════════════════════════════════════════
// MD5 签名
// ═══════════════════════════════════════════

#[no_mangle]
pub extern "C" fn nfc_sign(access_token:*const c_char,file_id:*const c_char,
    share_id:*const c_char,ts:*const c_char,
    out_sig:*mut c_char,sig_cap:u32)->i32
{
    let at=unsafe{cstr(access_token)};let fi=unsafe{cstr(file_id)};let si=unsafe{cstr(share_id)};let t=unsafe{cstr(ts)};
    let src=format!("AccessToken={}&Timestamp={}&dt=1&fileId={}&shareId={}",at,t,fi,si);
    let sig=md5_hex(src.as_bytes());
    if !out_sig.is_null()&&sig_cap>0{
        let b=sig.as_bytes();
        let n=b.len().min(sig_cap as usize-1);
        unsafe{ptr::copy_nonoverlapping(b.as_ptr(),out_sig as *mut u8,n);*out_sig.add(n)=0;}
    }
    0
}

#[no_mangle]
pub extern "C" fn nfc_md5(data:*const c_char)->*mut c_char{
    cstring(&md5_hex(unsafe{cstr(data)}.as_bytes()))
}

#[no_mangle]
pub extern "C" fn nfc_free(ptr:*mut c_char){
    if !ptr.is_null(){unsafe{let _=CString::from_raw(ptr);}}
}

// ═══════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════

unsafe fn cstr<'a>(p:*const c_char)->&'a str{
    if p.is_null(){return "";}
    CStr::from_ptr(p).to_str().unwrap_or("")
}

fn cstring(s:&str)->*mut c_char{
    CString::new(s).unwrap_or_default().into_raw()
}

/// 解析 JSON 字符串数组，如 ["a","b"]
fn parse_json_str_array(s: &str) -> Vec<String> {
    let s = s.trim();
    if !s.starts_with('[') { return vec![]; }
    let mut result = Vec::new();
    let mut in_str = false;
    let mut start = 0;
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'"' if !in_str => { in_str = true; start = i + 1; }
            b'"' if in_str => {
                result.push(s[start..i].to_string());
                in_str = false;
            }
            _ => {}
        }
    }
    result
}

/// 解析 JSON 数字数组，如 [100,200]
fn parse_json_u64_array(s: &str) -> Vec<u64> {
    let s = s.trim();
    if !s.starts_with('[') { return vec![]; }
    let mut result = Vec::new();
    let mut num_start: Option<usize> = None;
    for (i, ch) in s.char_indices() {
        if ch.is_ascii_digit() {
            if num_start.is_none() { num_start = Some(i); }
        } else if let Some(start) = num_start {
            result.push(s[start..i].parse().unwrap_or(0));
            num_start = None;
        }
    }
    if let Some(start) = num_start {
        result.push(s[start..].parse().unwrap_or(0));
    }
    result
}
