#!/usr/bin/env node
"use strict";

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");

const WEB_URL = "https://cloud.189.cn";
const API_URL = "https://api.cloud.189.cn";
const UPLOAD_URL = "https://upload.cloud.189.cn";
const APP_KEY = "600100422";
const ROOT_FOLDER_ID = "-11";
const DEFAULT_SLICE_SIZE = 10 * 1024 * 1024;
const USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36";

function nowMs() {
  return Date.now();
}

function shortText(value, limit = 320) {
  const text = String(value || "").replace(/\r/g, "\\r").replace(/\n/g, "\\n");
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}...`;
}

function md5Hex(text) {
  return crypto.createHash("md5").update(String(text), "utf8").digest("hex");
}

function md5Signature(params) {
  const entries = Object.entries(params || {}).filter(([, v]) => v !== undefined && v !== null);
  entries.sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  const signText = entries.map(([k, v]) => `${k}=${v}`).join("&");
  return md5Hex(signText);
}

function hmacSha1Hex(text, key) {
  return crypto.createHmac("sha1", Buffer.from(String(key), "utf8")).update(String(text), "utf8").digest("hex");
}

function aesEncryptHex(data, key16) {
  const cipher = crypto.createCipheriv("aes-128-ecb", Buffer.from(String(key16), "utf8"), null);
  cipher.setAutoPadding(true);
  const encrypted = Buffer.concat([cipher.update(Buffer.from(String(data), "utf8")), cipher.final()]);
  return encrypted.toString("hex");
}

function rsaEncryptBase64(text, pubKey) {
  const key = `-----BEGIN PUBLIC KEY-----\n${String(pubKey || "")}\n-----END PUBLIC KEY-----`;
  const encrypted = crypto.publicEncrypt(
    {
      key,
      padding: crypto.constants.RSA_PKCS1_PADDING,
    },
    Buffer.from(String(text), "utf8"),
  );
  return encrypted.toString("base64");
}

function uuidNoDash() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID().replace(/-/g, "");
  }
  return crypto.randomBytes(16).toString("hex");
}

function appendQuery(url, query) {
  const u = new URL(url);
  Object.entries(query || {}).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    u.searchParams.set(String(k), String(v));
  });
  return u.toString();
}

function parseJsonSafe(text) {
  try {
    return JSON.parse(String(text || ""));
  } catch (_err) {
    return null;
  }
}

function firstString(...values) {
  for (const value of values) {
    const text = String(value || "").trim();
    if (text) return text;
  }
  return "";
}

function ensureSuccessPayload(payload, action) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error(`${action} 返回格式异常`);
  }
  const codeRaw = payload.res_code ?? payload.resCode ?? payload.code ?? payload.errorCode;
  const code = String(codeRaw === undefined || codeRaw === null ? "" : codeRaw).trim();
  if (code && !["0", "SUCCESS", "success"].includes(code)) {
    const msg = firstString(
      payload.msg,
      payload.message,
      payload.res_message,
      payload.resMessage,
      payload.errorMsg,
      payload.errorMessage,
      payload.desc,
      payload.description,
    );
    throw new Error(`${action} 失败 code=${code}${msg ? ` msg=${msg}` : ""}`);
  }
}

function baseHeaders(cookie) {
  return {
    "User-Agent": USER_AGENT,
    Accept: "application/json;charset=UTF-8",
    Referer: `${WEB_URL}/web/main/`,
    Cookie: String(cookie || ""),
  };
}

async function requestOpenApi({ cookie, sessionKey, endpoint, method = "GET", query = {}, form = {} }) {
  const ts = String(nowMs());
  const signParams = {
    ...(String(method).toUpperCase() === "GET" ? query : { ...query, ...form }),
    Timestamp: ts,
    AppKey: APP_KEY,
  };
  const signature = md5Signature(signParams);

  let url = `${WEB_URL}${endpoint}`;
  const queryWithSession = { ...query, sessionKey };
  url = appendQuery(url, queryWithSession);

  const headers = {
    ...baseHeaders(cookie),
    "Sign-Type": "1",
    Signature: signature,
    Timestamp: ts,
    AppKey: APP_KEY,
  };

  const upperMethod = String(method).toUpperCase();
  const init = {
    method: upperMethod,
    headers,
    redirect: "follow",
  };

  if (upperMethod !== "GET") {
    const body = new URLSearchParams();
    Object.entries(form || {}).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      body.append(String(k), String(v));
    });
    headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8";
    init.body = body;
  }

  const resp = await fetch(url, init);
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`请求失败 status=${resp.status} endpoint=${endpoint} body=${shortText(text)}`);
  }

  const payload = parseJsonSafe(text);
  if (!payload) {
    throw new Error(`响应非 JSON endpoint=${endpoint} body=${shortText(text)}`);
  }
  return payload;
}

async function requestApiSigned({ cookie, accessToken, endpoint, query = {} }) {
  const ts = String(nowMs());
  const signParams = {
    ...query,
    Timestamp: ts,
    AccessToken: accessToken,
  };
  const signature = md5Signature(signParams);

  let url = `${API_URL}${endpoint}`;
  url = appendQuery(url, query);

  const headers = {
    ...baseHeaders(cookie),
    "Sign-Type": "1",
    Signature: signature,
    Timestamp: ts,
    Accesstoken: String(accessToken || ""),
  };

  const resp = await fetch(url, {
    method: "GET",
    headers,
    redirect: "follow",
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`请求失败 status=${resp.status} endpoint=${endpoint} body=${shortText(text)}`);
  }

  const payload = parseJsonSafe(text);
  if (!payload) {
    throw new Error(`响应非 JSON endpoint=${endpoint} body=${shortText(text)}`);
  }
  return payload;
}

async function requestJsonGet({ cookie, url, action = "requestJsonGet" }) {
  const resp = await fetch(String(url || ""), {
    method: "GET",
    headers: baseHeaders(cookie),
    redirect: "follow",
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`${action} 请求失败 status=${resp.status} body=${shortText(text)}`);
  }
  const payload = parseJsonSafe(text);
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error(`${action} 返回格式异常 body=${shortText(text)}`);
  }
  return payload;
}

async function listFolders({ cookie, sessionKey, parentFolderId }) {
  const folders = [];
  let pageNum = 1;
  while (pageNum <= 20) {
    const payload = await requestOpenApi({
      cookie,
      sessionKey,
      endpoint: "/api/open/file/listFiles.action",
      method: "GET",
      query: {
        pageSize: "60",
        pageNum: String(pageNum),
        mediaType: "0",
        folderId: String(parentFolderId),
        iconOption: "5",
        orderBy: "lastOpTime",
        descending: "true",
      },
    });
    ensureSuccessPayload(payload, "listFiles");

    const fileListAO = (payload && payload.fileListAO && typeof payload.fileListAO === "object") ? payload.fileListAO : {};
    const folderList = Array.isArray(fileListAO.folderList) ? fileListAO.folderList : [];
    folderList.forEach((item) => {
      if (!item || typeof item !== "object") return;
      const id = firstString(item.id, item.folderId, item.fileId);
      const name = firstString(item.name, item.folderName, item.fileName);
      if (!id || !name) return;
      folders.push({ id, name });
    });

    const totalCount = Number(fileListAO.count || 0);
    if (!folderList.length || totalCount <= 0) break;
    const pageCount = Math.max(1, Math.ceil(totalCount / 60));
    if (pageNum >= pageCount) break;
    pageNum += 1;
  }
  return folders;
}

async function listFiles({ cookie, sessionKey, parentFolderId }) {
  const files = [];
  let pageNum = 1;
  while (pageNum <= 30) {
    const payload = await requestOpenApi({
      cookie,
      sessionKey,
      endpoint: "/api/open/file/listFiles.action",
      method: "GET",
      query: {
        pageSize: "60",
        pageNum: String(pageNum),
        mediaType: "0",
        folderId: String(parentFolderId),
        iconOption: "5",
        orderBy: "lastOpTime",
        descending: "true",
      },
    });
    ensureSuccessPayload(payload, "listFiles");

    const fileListAO = (payload && payload.fileListAO && typeof payload.fileListAO === "object") ? payload.fileListAO : {};
    const fileList = Array.isArray(fileListAO.fileList) ? fileListAO.fileList : [];
    fileList.forEach((item) => {
      if (!item || typeof item !== "object") return;
      const fileId = firstString(item.id, item.fileId, item.fileID);
      const name = firstString(item.name, item.fileName);
      if (!fileId || !name) return;
      const size = Number(item.size || item.fileSize || 0);
      files.push({
        file_id: fileId,
        name,
        size: Number.isFinite(size) ? Math.max(0, Math.floor(size)) : 0,
        last_op_time: firstString(item.lastOpTime, item.lastUpdateTime, item.modifyDate, item.createDate),
      });
    });

    const totalCount = Number(fileListAO.count || 0);
    if (!fileList.length || totalCount <= 0) break;
    const pageCount = Math.max(1, Math.ceil(totalCount / 60));
    if (pageNum >= pageCount) break;
    pageNum += 1;
  }
  return files;
}

function normalizeFolderName(value) {
  const text = String(value || "").trim().replace(/[\\/]+/g, "_");
  return text;
}

async function createFolderCandidates({ cookie, sessionKey, parentFolderId, folderName }) {
  const attempts = [];
  const taskInfosJson = JSON.stringify([{ fileName: String(folderName), isFolder: 1 }]);
  const candidates = [
    {
      endpoint: "/api/open/file/createFolder.action",
      method: "POST",
      form: { folderName: String(folderName), parentFolderId: String(parentFolderId) },
    },
    {
      endpoint: "/api/open/file/createFolder.action",
      method: "POST",
      form: { name: String(folderName), parentFolderId: String(parentFolderId) },
    },
    {
      endpoint: "/api/open/file/createFolder.action",
      method: "POST",
      form: { folderName: String(folderName), folderId: String(parentFolderId) },
    },
    {
      endpoint: "/api/open/batch/createBatchTask.action",
      method: "POST",
      form: {
        type: "CREATE_FOLDER",
        targetFolderId: String(parentFolderId),
        taskInfos: taskInfosJson,
      },
    },
    {
      endpoint: "/api/open/batch/createBatchTask.action",
      method: "POST",
      form: {
        type: "MKDIR",
        targetFolderId: String(parentFolderId),
        taskInfos: taskInfosJson,
      },
    },
  ];

  for (const item of candidates) {
    try {
      const payload = await requestOpenApi({
        cookie,
        sessionKey,
        endpoint: item.endpoint,
        method: item.method,
        form: item.form,
      });
      ensureSuccessPayload(payload, `createFolder:${item.endpoint}`);
      attempts.push({ ok: true, endpoint: item.endpoint, method: item.method });
    } catch (err) {
      attempts.push({ ok: false, endpoint: item.endpoint, method: item.method, error: shortText(err && err.message ? err.message : String(err)) });
    }
  }

  return attempts;
}

async function ensureFolderPath({ cookie, sessionKey, parts }) {
  let parentFolderId = ROOT_FOLDER_ID;
  const trace = [];

  for (const rawPart of parts || []) {
    const folderName = normalizeFolderName(rawPart);
    if (!folderName) continue;

    const folders = await listFolders({ cookie, sessionKey, parentFolderId });
    let found = folders.find((item) => item.name === folderName);
    if (!found) {
      const attempts = await createFolderCandidates({ cookie, sessionKey, parentFolderId, folderName });
      const refreshed = await listFolders({ cookie, sessionKey, parentFolderId });
      found = refreshed.find((item) => item.name === folderName);
      trace.push({
        step: "ensure_folder",
        parent_folder_id: parentFolderId,
        folder_name: folderName,
        created: Boolean(found),
        attempts,
      });
    } else {
      trace.push({
        step: "ensure_folder",
        parent_folder_id: parentFolderId,
        folder_name: folderName,
        created: false,
        found: true,
      });
    }

    if (!found || !found.id) {
      throw new Error(`无法创建或定位目录: ${folderName}`);
    }
    parentFolderId = String(found.id);
  }

  return { folderId: parentFolderId, trace };
}

async function findFolderPath({ cookie, sessionKey, parts }) {
  let parentFolderId = ROOT_FOLDER_ID;
  const trace = [];
  for (const rawPart of parts || []) {
    const folderName = normalizeFolderName(rawPart);
    if (!folderName) continue;
    const folders = await listFolders({ cookie, sessionKey, parentFolderId });
    const found = folders.find((item) => item.name === folderName);
    trace.push({
      step: "find_folder",
      parent_folder_id: parentFolderId,
      folder_name: folderName,
      found: Boolean(found),
    });
    if (!found || !found.id) {
      return {
        ok: false,
        folderId: "",
        trace,
        reason: "folder_not_found",
        missing_name: folderName,
      };
    }
    parentFolderId = String(found.id);
  }
  return {
    ok: true,
    folderId: parentFolderId,
    trace,
  };
}

async function getRsaKey({ cookie, accessToken }) {
  const attempts = [];
  const candidates = [
    {
      source: "cloud_unsigned",
      run: async () => {
        const url = appendQuery(`${WEB_URL}/api/security/generateRsaKey.action`, { noCache: String(nowMs()) });
        return requestJsonGet({ cookie, url, action: "generateRsaKey.cloud" });
      },
    },
    {
      source: "api_signed",
      run: async () => requestApiSigned({
        cookie,
        accessToken,
        endpoint: "/security/generateRsaKey.action",
      }),
    },
  ];

  for (const candidate of candidates) {
    try {
      const payload = await candidate.run();
      ensureSuccessPayload(payload, `generateRsaKey.${candidate.source}`);
      const pubKey = firstString(payload.pubKey, payload.data && payload.data.pubKey);
      const pkId = firstString(payload.pkId, payload.data && payload.data.pkId);
      if (pubKey && pkId) {
        return { pubKey, pkId };
      }
      attempts.push({
        source: candidate.source,
        ok: false,
        message: "响应缺少 pubKey/pkId",
      });
    } catch (err) {
      attempts.push({
        source: candidate.source,
        ok: false,
        message: shortText(err && err.message ? err.message : String(err)),
      });
    }
  }

  throw new Error(`RSA 公钥响应缺少 pubKey/pkId attempts=${JSON.stringify(attempts)}`);
}

async function uploadRequest({ cookie, accessToken, sessionKey, rsa, uri, form }) {
  const requestDate = String(nowMs());
  const requestId = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : uuidNoDash();
  const encryptSeed = uuidNoDash().slice(0, 16 + Math.floor(Math.random() * 16));
  const aesKey = encryptSeed.slice(0, 16);

  const formText = Object.entries(form || {}).map(([k, v]) => `${k}=${v}`).join("&");
  const encryptedParamsHex = aesEncryptHex(formText, aesKey);

  const signText = `SessionKey=${sessionKey}&Operate=GET&RequestURI=${uri}&Date=${requestDate}&params=${encryptedParamsHex}`;
  const signature = hmacSha1Hex(signText, encryptSeed);
  const encryptionText = rsaEncryptBase64(encryptSeed, rsa.pubKey);

  const headers = {
    ...baseHeaders(cookie),
    SessionKey: String(sessionKey || ""),
    Signature: signature,
    "X-Request-Date": requestDate,
    "X-Request-ID": requestId,
    EncryptionText: encryptionText,
    PkId: String(rsa.pkId || ""),
  };

  const url = `${UPLOAD_URL}${uri}?params=${encryptedParamsHex}`;
  const resp = await fetch(url, {
    method: "GET",
    headers,
    redirect: "follow",
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`上传接口失败 status=${resp.status} uri=${uri} body=${shortText(text)}`);
  }

  const payload = parseJsonSafe(text);
  if (!payload) {
    throw new Error(`上传接口返回非 JSON uri=${uri} body=${shortText(text)}`);
  }
  if (String(payload.code || "").trim() !== "SUCCESS") {
    throw new Error(`上传接口业务失败 uri=${uri} msg=${firstString(payload.msg, payload.message, JSON.stringify(payload))}`);
  }
  return payload;
}

function parseRequestHeaderPairs(rawHeader) {
  const headers = {};
  const text = decodeURIComponent(String(rawHeader || ""));
  text.split("&").forEach((pair) => {
    if (!pair) return;
    const idx = pair.indexOf("=");
    if (idx <= 0) return;
    const key = pair.slice(0, idx);
    const value = pair.slice(idx + 1);
    headers[String(key)] = String(value);
  });
  return headers;
}

async function uploadFileSlices({ cookie, accessToken, sessionKey, rsa, localFilePath, parentFolderId, remoteName, sliceSize = DEFAULT_SLICE_SIZE }) {
  const stat = fs.statSync(localFilePath);
  if (!stat.isFile()) {
    throw new Error("待上传文件无效");
  }

  const fileSize = Number(stat.size || 0);
  const chunkSize = Math.max(1024 * 1024, Number(sliceSize || DEFAULT_SLICE_SIZE));
  const chunkCount = Math.max(1, Math.ceil(fileSize / chunkSize));

  const initPayload = await uploadRequest({
    cookie,
    accessToken,
    sessionKey,
    rsa,
    uri: "/person/initMultiUpload",
    form: {
      parentFolderId: String(parentFolderId || ROOT_FOLDER_ID),
      fileName: encodeURIComponent(String(remoteName || path.basename(localFilePath))),
      fileSize: String(fileSize),
      sliceSize: String(chunkSize),
      lazyCheck: "1",
    },
  });

  const uploadFileId = firstString(
    initPayload && initPayload.data && initPayload.data.uploadFileId,
    initPayload && initPayload.uploadFileId,
  );
  if (!uploadFileId) {
    throw new Error("初始化上传缺少 uploadFileId");
  }

  const fd = fs.openSync(localFilePath, "r");
  const md5PartHexUpper = [];
  const fileMd5Hasher = crypto.createHash("md5");

  try {
    for (let i = 1; i <= chunkCount; i += 1) {
      const offset = (i - 1) * chunkSize;
      const size = Math.min(chunkSize, fileSize - offset);
      const buffer = Buffer.alloc(size);
      const readBytes = fs.readSync(fd, buffer, 0, size, offset);
      const chunkBuffer = readBytes === size ? buffer : buffer.slice(0, readBytes);

      const md5Bytes = crypto.createHash("md5").update(chunkBuffer).digest();
      const md5HexUpper = md5Bytes.toString("hex").toUpperCase();
      const md5Base64 = md5Bytes.toString("base64");
      md5PartHexUpper.push(md5HexUpper);
      fileMd5Hasher.update(chunkBuffer);

      const urlPayload = await uploadRequest({
        cookie,
        accessToken,
        sessionKey,
        rsa,
        uri: "/person/getMultiUploadUrls",
        form: {
          partInfo: `${i}-${md5Base64}`,
          uploadFileId,
        },
      });

      const uploadUrls = (urlPayload && typeof urlPayload === "object" && urlPayload.uploadUrls && typeof urlPayload.uploadUrls === "object")
        ? urlPayload.uploadUrls
        : {};
      const uploadData = uploadUrls[`partNumber_${i}`];
      const requestURL = uploadData && uploadData.requestURL ? String(uploadData.requestURL) : "";
      if (!requestURL) {
        throw new Error(`上传分片 URL 缺失 part=${i}`);
      }

      const putHeaders = parseRequestHeaderPairs(uploadData.requestHeader || "");
      const putResp = await fetch(requestURL, {
        method: "PUT",
        headers: putHeaders,
        body: chunkBuffer,
      });
      if (!putResp.ok) {
        const putBody = await putResp.text().catch(() => "");
        throw new Error(`分片上传失败 part=${i} status=${putResp.status} body=${shortText(putBody)}`);
      }
    }
  } finally {
    fs.closeSync(fd);
  }

  const fileMd5 = fileMd5Hasher.digest("hex");
  const sliceMd5 = fileSize <= chunkSize
    ? fileMd5
    : md5Hex(md5PartHexUpper.join("\n"));

  const commitPayload = await uploadRequest({
    cookie,
    accessToken,
    sessionKey,
    rsa,
    uri: "/person/commitMultiUploadFile",
    form: {
      uploadFileId,
      fileMd5,
      sliceMd5,
      lazyCheck: "1",
      opertype: "3",
    },
  });

  const userFileId = firstString(
    commitPayload && commitPayload.file && commitPayload.file.userFileId,
    commitPayload && commitPayload.data && commitPayload.data.userFileId,
    commitPayload && commitPayload.userFileId,
  );

  return {
    userFileId,
    fileSize,
    chunkCount,
    uploadFileId,
  };
}

function extractDownloadUrl(payload) {
  if (!payload || typeof payload !== "object") return "";
  const direct = firstString(payload.fileDownloadUrl, payload.downloadUrl, payload.url);
  if (direct) return direct;
  const data = (payload.data && typeof payload.data === "object") ? payload.data : {};
  return firstString(data.fileDownloadUrl, data.downloadUrl, data.url);
}

async function getCloudFileDownloadUrl({ cookie, accessToken, sessionKey, fileId }) {
  const candidates = [
    async () => requestOpenApi({
      cookie,
      sessionKey,
      endpoint: "/api/open/file/getFileDownloadUrl.action",
      method: "GET",
      query: { fileId: String(fileId || ""), dt: "1" },
    }),
    async () => requestOpenApi({
      cookie,
      sessionKey,
      endpoint: "/api/open/file/getFileDownloadUrl.action",
      method: "GET",
      query: { fileId: String(fileId || "") },
    }),
    async () => requestApiSigned({
      cookie,
      accessToken,
      endpoint: "/open/file/getFileDownloadUrl.action",
      query: { fileId: String(fileId || ""), dt: "1" },
    }),
    async () => requestApiSigned({
      cookie,
      accessToken,
      endpoint: "/open/file/getFileDownloadUrl.action",
      query: { fileId: String(fileId || "") },
    }),
  ];

  const attempts = [];
  for (const fn of candidates) {
    try {
      const payload = await fn();
      ensureSuccessPayload(payload, "getFileDownloadUrl");
      const url = extractDownloadUrl(payload);
      if (url) return url;
      attempts.push({ ok: false, reason: "missing_url" });
    } catch (err) {
      attempts.push({ ok: false, reason: shortText(err && err.message ? err.message : String(err)) });
    }
  }
  throw new Error(`获取文件下载地址失败 attempts=${JSON.stringify(attempts)}`);
}

async function downloadUrlToFile({ cookie, downloadUrl, localFilePath }) {
  const targetPath = path.resolve(String(localFilePath || "").trim());
  if (!targetPath) {
    throw new Error("本地下载路径无效");
  }
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });

  const resp = await fetch(String(downloadUrl || ""), {
    method: "GET",
    headers: baseHeaders(cookie),
    redirect: "follow",
  });
  if (!resp.ok || !resp.body) {
    const body = await resp.text().catch(() => "");
    throw new Error(`下载失败 status=${resp.status} body=${shortText(body)}`);
  }

  const nodeReadable = Readable.fromWeb(resp.body);
  await pipeline(nodeReadable, fs.createWriteStream(targetPath));
  const stat = fs.statSync(targetPath);
  return {
    local_file_path: targetPath,
    file_size: Number(stat.size || 0),
  };
}

async function runUpload(payload) {
  const cookie = String(payload.cookie || "").trim();
  const accessToken = String(payload.access_token || "").trim();
  const sessionKey = String(payload.session_key || "").trim();
  const localFilePath = path.resolve(String(payload.local_file_path || "").trim());
  const remoteName = String(payload.remote_name || "").trim() || path.basename(localFilePath);
  const remoteFolderParts = Array.isArray(payload.remote_folder_parts)
    ? payload.remote_folder_parts.map((item) => normalizeFolderName(item)).filter(Boolean)
    : [];

  if (!cookie) throw new Error("缺少 cookie");
  if (!accessToken) throw new Error("缺少 access_token");
  if (!sessionKey) throw new Error("缺少 session_key");
  if (!localFilePath || !fs.existsSync(localFilePath) || !fs.statSync(localFilePath).isFile()) {
    throw new Error("待上传文件不存在");
  }

  const folderInfo = await ensureFolderPath({
    cookie,
    sessionKey,
    parts: ["FreedeckCloudSaves", ...remoteFolderParts],
  });

  const rsa = await getRsaKey({ cookie, accessToken });
  const uploadInfo = await uploadFileSlices({
    cookie,
    accessToken,
    sessionKey,
    rsa,
    localFilePath,
    parentFolderId: folderInfo.folderId,
    remoteName,
  });

  const cloudPath = `/FreedeckCloudSaves/${remoteFolderParts.join("/")}${remoteFolderParts.length ? "/" : ""}${remoteName}`;
  return {
    folder_id: String(folderInfo.folderId),
    cloud_path: cloudPath,
    remote_name: remoteName,
    user_file_id: String(uploadInfo.userFileId || ""),
    file_size: Number(uploadInfo.fileSize || 0),
    chunk_count: Number(uploadInfo.chunkCount || 0),
    ensure_trace: folderInfo.trace,
  };
}

function normalizeRemoteFolderParts(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => normalizeFolderName(item)).filter(Boolean);
}

async function runListVersions(payload) {
  const cookie = String(payload.cookie || "").trim();
  const sessionKey = String(payload.session_key || "").trim();
  const remoteFolderParts = normalizeRemoteFolderParts(payload.remote_folder_parts);

  if (!cookie) throw new Error("缺少 cookie");
  if (!sessionKey) throw new Error("缺少 session_key");

  const folderResult = await findFolderPath({
    cookie,
    sessionKey,
    parts: ["FreedeckCloudSaves", ...remoteFolderParts],
  });
  if (!folderResult.ok) {
    return {
      exists: false,
      folder_id: "",
      files: [],
      trace: folderResult.trace || [],
      reason: String(folderResult.reason || "folder_not_found"),
    };
  }

  const files = await listFiles({
    cookie,
    sessionKey,
    parentFolderId: folderResult.folderId,
  });
  const archives = files.filter((item) => String(item.name || "").toLowerCase().endsWith(".7z"));
  return {
    exists: true,
    folder_id: String(folderResult.folderId || ""),
    files: archives,
    trace: folderResult.trace || [],
  };
}

async function runDownloadFile(payload) {
  const cookie = String(payload.cookie || "").trim();
  const accessToken = String(payload.access_token || "").trim();
  const sessionKey = String(payload.session_key || "").trim();
  const fileId = String(payload.file_id || "").trim();
  const localFilePath = String(payload.local_file_path || "").trim();

  if (!cookie) throw new Error("缺少 cookie");
  if (!accessToken) throw new Error("缺少 access_token");
  if (!sessionKey) throw new Error("缺少 session_key");
  if (!fileId) throw new Error("缺少 file_id");
  if (!localFilePath) throw new Error("缺少 local_file_path");

  const downloadUrl = await getCloudFileDownloadUrl({
    cookie,
    accessToken,
    sessionKey,
    fileId,
  });
  const fileInfo = await downloadUrlToFile({
    cookie,
    downloadUrl,
    localFilePath,
  });
  return {
    file_id: fileId,
    download_url: downloadUrl,
    ...fileInfo,
  };
}

function readStdinJson() {
  return new Promise((resolve, reject) => {
    const chunks = [];
    process.stdin.on("data", (chunk) => chunks.push(chunk));
    process.stdin.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(JSON.parse(text));
      } catch (err) {
        reject(err);
      }
    });
    process.stdin.on("error", reject);
  });
}

(async () => {
  try {
    const input = await readStdinJson();
    const payload = input || {};
    const action = String(payload.action || "upload").trim().toLowerCase();
    let data;
    if (action === "upload") {
      data = await runUpload(payload);
    } else if (action === "list_versions") {
      data = await runListVersions(payload);
    } else if (action === "download_file") {
      data = await runDownloadFile(payload);
    } else {
      throw new Error(`不支持的 action: ${action}`);
    }
    process.stdout.write(JSON.stringify({ ok: true, data }));
  } catch (err) {
    const diagnostics = {
      error_type: err && err.name ? String(err.name) : "Error",
      message: shortText(err && err.message ? err.message : String(err)),
    };
    process.stdout.write(JSON.stringify({ ok: false, error: diagnostics.message, diagnostics }));
    process.exitCode = 1;
  }
})();
