"use strict";

import { URL } from "node:url";

const USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

function shortText(text, limit = 320) {
  const raw = String(text || "").replace(/\r/g, "\\r").replace(/\n/g, "\\n");
  if (raw.length <= limit) return raw;
  return `${raw.slice(0, limit)}...`;
}

function detectBodyType(text) {
  const raw = String(text || "").trim();
  if (!raw) return "empty";
  const head = raw.slice(0, 120).toLowerCase();
  if (raw.startsWith("{") || raw.startsWith("[")) return "json";
  if (/^\s*[\w.$]+\(([\s\S]+)\)\s*;?\s*$/.test(raw)) return "jsonp";
  if (raw.startsWith("<")) {
    if (head.includes("<!doctype html") || head.includes("<html")) return "html";
    return "xml";
  }
  return "text";
}

function parseJsonLike(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;

  try {
    return JSON.parse(raw);
  } catch (_err) {
    // ignore
  }

  const jsonp = raw.match(/^\s*[\w.$]+\(([\s\S]+)\)\s*;?\s*$/);
  if (jsonp) {
    try {
      return JSON.parse(String(jsonp[1] || "").trim());
    } catch (_err) {
      return null;
    }
  }
  return null;
}

function getDeepString(payload, keys) {
  if (!payload || typeof payload !== "object") return "";
  const wanted = new Set(keys.map((k) => String(k || "").toLowerCase()));
  const stack = [payload];
  const visited = new Set();

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current)) continue;
    visited.add(current);

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") stack.push(item);
      }
      continue;
    }

    for (const [rawKey, rawValue] of Object.entries(current)) {
      const key = String(rawKey || "").toLowerCase();
      if (wanted.has(key)) {
        const text = String(rawValue || "").trim();
        if (text) return text;
      }
      if (rawValue && typeof rawValue === "object") stack.push(rawValue);
    }
  }

  return "";
}

function getDeepArray(payload, keys) {
  if (!payload || typeof payload !== "object") return [];
  const wanted = new Set(keys.map((k) => String(k || "").toLowerCase()));
  const stack = [payload];
  const visited = new Set();

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current)) continue;
    visited.add(current);

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") stack.push(item);
      }
      continue;
    }

    for (const [rawKey, rawValue] of Object.entries(current)) {
      const key = String(rawKey || "").toLowerCase();
      if (wanted.has(key) && Array.isArray(rawValue)) return rawValue;
      if (rawValue && typeof rawValue === "object") stack.push(rawValue);
    }
  }

  return [];
}

function asInt(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (Number.isFinite(parsed)) return parsed;
  return fallback;
}

function toFolderFlag(value) {
  const text = String(value ?? "").toLowerCase().trim();
  return text === "1" || text === "true";
}

function extractShareIdFromText(text) {
  const raw = String(text || "");
  if (!raw) return "";
  const patterns = [
    /"share[Ii][Dd]"\s*:\s*"([A-Za-z0-9_-]{4,})"/,
    /\bshare[Ii][Dd]\s*[:=]\s*['"]?([A-Za-z0-9_-]{4,})['"]?/,
    /[?&]shareId=([A-Za-z0-9_-]{4,})/i,
  ];
  for (const pattern of patterns) {
    const matched = raw.match(pattern);
    if (matched && matched[1]) return String(matched[1]).trim();
  }
  return "";
}

function buildHeaders(cookie, extras = {}) {
  return Object.assign(
    {
      "User-Agent": USER_AGENT,
      Accept: "application/json;charset=UTF-8",
      Cookie: String(cookie || ""),
    },
    extras,
  );
}

async function requestText(url, cookie, extraHeaders = {}) {
  if (typeof fetch !== "function") {
    throw new Error("node_fetch_unavailable");
  }

  const response = await fetch(url, {
    method: "GET",
    headers: buildHeaders(cookie, extraHeaders),
    redirect: "follow",
  });
  const text = await response.text();
  return {
    status: Number(response.status || 0),
    url: String(response.url || url),
    text,
    bodyType: detectBodyType(text),
    payload: parseJsonLike(text),
  };
}

function mapFilesFromPayload(payload) {
  const list = getDeepArray(payload, ["fileList", "files", "rows", "list"]);
  const files = [];
  if (Array.isArray(list)) {
    for (const item of list) {
      if (!item || typeof item !== "object") continue;
      const fileId = String(item.id || item.fileId || "").trim();
      if (!fileId) continue;
      files.push({
        file_id: fileId,
        name: String(item.name || item.fileName || `file-${fileId}`),
        size: Math.max(0, asInt(item.size ?? item.fileSize, 0)),
        is_folder: toFolderFlag(item.isFolder),
      });
    }
  }
  return files;
}

function appendAttempt(attempts, payload) {
  attempts.push(
    Object.assign(
      {
        ok: false,
        status: 0,
        endpoint: "",
        step: "",
      },
      payload || {},
    ),
  );
}

async function run(input) {
  const shareUrl = String(input.share_url || "").trim();
  const cookie = String(input.cookie || "").trim();
  if (!shareUrl) throw new Error("share_url_empty");
  if (!cookie) throw new Error("cookie_empty");

  let parsedUrl;
  try {
    parsedUrl = new URL(shareUrl);
  } catch (_err) {
    throw new Error("share_url_invalid");
  }

  const host = String(parsedUrl.hostname || "").toLowerCase();
  if (host !== "cloud.189.cn" && host !== "www.cloud.189.cn") {
    throw new Error("share_host_invalid");
  }

  const pathParts = String(parsedUrl.pathname || "")
    .split("/")
    .filter((item) => Boolean(String(item || "").trim()));
  if (pathParts.length < 2 || pathParts[0] !== "t") {
    throw new Error("share_path_invalid");
  }

  const shareCode = String(pathParts[1] || "").trim();
  const pwd = String(parsedUrl.searchParams.get("pwd") || "").trim();
  const noCache = String(Date.now());
  const attempts = [];

  let shareId = "";
  let rootFileId = "";
  let isFolder = false;
  let infoPayload = {};
  let checkPayload = {};

  if (pwd) {
    const checkUrl =
      "https://cloud.189.cn/api/open/share/checkAccessCode.action"
      + `?noCache=${encodeURIComponent(noCache)}`
      + `&shareCode=${encodeURIComponent(shareCode)}`
      + `&accessCode=${encodeURIComponent(pwd)}`;
    const checkResp = await requestText(checkUrl, cookie);
    const checkObj = checkResp.payload && typeof checkResp.payload === "object" ? checkResp.payload : {};
    checkPayload = checkObj;
    shareId = getDeepString(checkObj, ["shareId", "shareID", "shareid"]) || shareId;
    appendAttempt(attempts, {
      step: "check_access_code_primary_js",
      endpoint: "/api/open/share/checkAccessCode.action",
      ok: Boolean(shareId),
      status: checkResp.status,
      body_type: checkResp.bodyType,
      body_preview: shortText(checkResp.text),
      share_id: shareId || "",
    });
  } else {
    appendAttempt(attempts, {
      step: "check_access_code_primary_js",
      endpoint: "/api/open/share/checkAccessCode.action",
      ok: false,
      status: 0,
      message: "pwd_missing_skip",
    });
  }

  const infoUrl =
    "https://cloud.189.cn/api/open/share/getShareInfoByCodeV2.action"
    + `?noCache=${encodeURIComponent(noCache)}`
    + `&shareCode=${encodeURIComponent(shareCode)}`;
  const infoResp = await requestText(infoUrl, cookie);
  const infoObj = infoResp.payload && typeof infoResp.payload === "object" ? infoResp.payload : {};
  infoPayload = infoObj;
  shareId = getDeepString(infoObj, ["shareId", "shareID", "shareid"]) || shareId;
  rootFileId = getDeepString(infoObj, ["fileId", "fileID", "fileid"]) || rootFileId;
  isFolder = toFolderFlag(getDeepString(infoObj, ["isFolder"]));
  appendAttempt(attempts, {
    step: "get_share_info_js",
    endpoint: "/api/open/share/getShareInfoByCodeV2.action",
    ok: Boolean(shareId),
    status: infoResp.status,
    body_type: infoResp.bodyType,
    body_preview: shortText(infoResp.text),
    share_id: shareId || "",
  });

  if (!shareId) {
    shareId = extractShareIdFromText(infoResp.text);
    if (!shareId) {
      shareId = extractShareIdFromText(String(infoResp.url || ""));
    }
  }
  if (!rootFileId) rootFileId = shareId;

  if (!shareId) {
    return {
      ok: false,
      error: "js_share_parse_failed_no_shareid",
      diagnostics: {
        share_code: shareCode,
        share_url: shareUrl,
        attempts,
        check_payload_keys: Object.keys(checkPayload || {}),
        info_payload_keys: Object.keys(infoPayload || {}),
      },
    };
  }

  let listUrl = "";
  if (isFolder) {
    listUrl =
      "https://cloud.189.cn/api/open/share/listShareDir.action"
      + `?noCache=${encodeURIComponent(noCache)}`
      + "&pageNum=1&pageSize=60"
      + `&fileId=${encodeURIComponent(shareId)}`
      + `&shareDirFileId=${encodeURIComponent(shareId)}`
      + "&isFolder=true"
      + `&shareId=${encodeURIComponent(shareId)}`
      + "&shareMode=1&iconOption=5"
      + "&orderBy=lastOpTime&descending=true"
      + `&accessCode=${encodeURIComponent(pwd)}`;
  } else {
    listUrl =
      "https://cloud.189.cn/api/open/share/listShareDir.action"
      + `?noCache=${encodeURIComponent(noCache)}`
      + `&fileId=${encodeURIComponent(rootFileId || shareId)}`
      + "&shareMode=1&isFolder=false"
      + `&shareId=${encodeURIComponent(shareId)}`
      + "&iconOption=5&pageNum=1&pageSize=10"
      + `&accessCode=${encodeURIComponent(pwd)}`;
  }

  const listResp = await requestText(listUrl, cookie, { "Sign-Type": "1" });
  const listObj = listResp.payload && typeof listResp.payload === "object" ? listResp.payload : {};
  appendAttempt(attempts, {
    step: "list_share_dir_js",
    endpoint: "/api/open/share/listShareDir.action",
    ok: listResp.status >= 200 && listResp.status < 400,
    status: listResp.status,
    body_type: listResp.bodyType,
    body_preview: shortText(listResp.text),
    share_id: shareId,
  });

  const files = mapFilesFromPayload(listObj);
  if (files.length === 0 && rootFileId) {
    files.push({
      file_id: String(rootFileId),
      name: String(getDeepString(infoObj, ["name", "fileName"]) || "single-file"),
      size: Math.max(0, asInt(getDeepString(infoObj, ["size", "fileSize"]), 0)),
      is_folder: false,
    });
  }

  return {
    ok: true,
    data: {
      share_code: shareCode,
      share_id: shareId,
      pwd,
      files,
    },
    diagnostics: {
      attempts,
    },
  };
}

async function readAllStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf-8");
}

async function main() {
  const raw = await readAllStdin();
  let input = {};
  try {
    input = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    process.stdout.write(JSON.stringify({ ok: false, error: "stdin_json_invalid" }));
    return;
  }

  try {
    const result = await run(input);
    process.stdout.write(JSON.stringify(result));
  } catch (err) {
    process.stdout.write(
      JSON.stringify({
        ok: false,
        error: String((err && err.message) || err || "js_resolver_exception"),
      }),
    );
  }
}

void main();
