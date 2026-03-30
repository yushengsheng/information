function buildNonJsonError(response, raw) {
  const error = new Error("服务器返回了非 JSON 响应，可能仍连接到旧版服务");
  error.code = "NON_JSON";
  error.status = response.status;
  error.raw = raw;
  return error;
}

function buildHttpError(response, data, raw) {
  const error = new Error(data?.error || `HTTP ${response.status}`);
  error.code = "HTTP";
  error.status = response.status;
  error.detail = data?.detail || "";
  error.raw = raw;
  return error;
}

async function readJsonResponse(response) {
  const raw = await response.text();
  let data = {};
  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch (_error) {
      throw buildNonJsonError(response, raw);
    }
  }
  if (!response.ok) {
    throw buildHttpError(response, data, raw);
  }
  return data;
}

export async function getJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  return readJsonResponse(response);
}

export async function postJson(url, payload, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  const response = await fetch(url, {
    ...options,
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  return readJsonResponse(response);
}
