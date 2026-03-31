from __future__ import annotations

import json
import os
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from services.intel.store import load_intel_config, load_intel_secrets
from services.intel.text import normalize_text, normalize_tweet_text, populate_display_text, short_summary_text, translate_text_to_chinese

DEFAULT_SUMMARY_MODE = "ai_first"
DEFAULT_SUMMARY_MODEL = "gpt-5.4"
DEFAULT_SUMMARY_FALLBACK = "translate_truncate"
SUMMARY_MODEL_FALLBACKS = ("gpt-5.3-codex", "gpt-5.2-codex")
MAX_AI_SUMMARY_ITEMS = 24
AI_SUMMARY_BATCH_SIZE = 4
AI_SUMMARY_WORKERS = 2
AI_SUMMARY_PRIMARY_TIMEOUT_SECONDS = 20
AI_SUMMARY_RETRY_TIMEOUT_SECONDS = 14


def normalize_summary_mode(value: object) -> str:
    mode = normalize_text(value).lower()
    if mode in {"fallback_only", "rule_only", "fallback"}:
        return "fallback_only"
    return DEFAULT_SUMMARY_MODE


def normalize_summary_model(value: object) -> str:
    model = normalize_text(value)
    return model or DEFAULT_SUMMARY_MODEL


def normalize_summary_config(raw: object) -> dict[str, str]:
    data = raw if isinstance(raw, dict) else {}
    return {
        "mode": normalize_summary_mode(data.get("mode")),
        "model": normalize_summary_model(data.get("model")),
    }


def _pretty_home_path(path: Path) -> str:
    home = Path.home().resolve()
    try:
        return f"~/{path.resolve().relative_to(home)}"
    except Exception:
        return str(path)


def _normalize_base_url(value: object) -> str:
    raw = normalize_text(value).rstrip("/")
    if not raw:
        return ""
    if raw.endswith("/v1"):
        return raw
    return f"{raw}/v1"


def _load_json_file(path: Path) -> dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _dedupe_nonempty_strings(values: list[object]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _resolve_from_env() -> dict[str, object]:
    api_key = normalize_text(os.environ.get("INTEL_SUMMARY_API_KEY"))
    base_url = _normalize_base_url(os.environ.get("INTEL_SUMMARY_BASE_URL"))
    if not api_key or not base_url:
        return {}
    return {
        "provider": normalize_text(os.environ.get("INTEL_SUMMARY_PROVIDER")) or "gmn",
        "base_url": base_url,
        "api_key": api_key,
        "credential_source": "环境变量",
    }


def _resolve_model_candidates_from_env() -> list[str]:
    raw = normalize_text(os.environ.get("INTEL_SUMMARY_MODEL_CANDIDATES"))
    if not raw:
        return []
    return _dedupe_nonempty_strings(raw.split(","))


def _resolve_from_app_secrets() -> dict[str, object]:
    secrets = load_intel_secrets()
    api_key = normalize_text(secrets.get("summary_api_key"))
    base_url = _normalize_base_url(secrets.get("summary_base_url"))
    if not api_key or not base_url:
        return {}
    return {
        "provider": normalize_text(secrets.get("summary_provider")) or "gmn",
        "base_url": base_url,
        "api_key": api_key,
        "credential_source": "项目 secrets",
    }


def _resolve_from_ccman(path: Path) -> dict[str, object]:
    data = _load_json_file(path)
    providers = data.get("providers") if isinstance(data.get("providers"), list) else []
    current_id = normalize_text(data.get("currentProviderId"))
    selected: dict[str, object] | None = None
    for item in providers:
        if not isinstance(item, dict):
            continue
        if current_id and normalize_text(item.get("id")) == current_id:
            selected = item
            break
        if normalize_text(item.get("name")).lower() == "gmn" and selected is None:
            selected = item
    if not isinstance(selected, dict):
        return {}
    api_key = normalize_text(selected.get("apiKey"))
    base_url = _normalize_base_url(selected.get("baseUrl"))
    if not api_key or not base_url:
        return {}
    return {
        "provider": normalize_text(selected.get("name")) or "gmn",
        "base_url": base_url,
        "api_key": api_key,
        "credential_source": _pretty_home_path(path),
    }


def _resolve_model_candidates_from_ccman(path: Path) -> list[str]:
    data = _load_json_file(path)
    presets = data.get("presets") if isinstance(data.get("presets"), list) else []
    models: list[object] = []
    for preset in presets:
        if not isinstance(preset, dict):
            continue
        models.append(preset.get("model"))
        models.append(preset.get("modelId"))
    return _dedupe_nonempty_strings(models)


def _resolve_from_opencode(path: Path) -> dict[str, object]:
    data = _load_json_file(path)
    provider = data.get("provider") if isinstance(data.get("provider"), dict) else {}
    openai_cfg = provider.get("openai") if isinstance(provider, dict) and isinstance(provider.get("openai"), dict) else {}
    options = openai_cfg.get("options") if isinstance(openai_cfg.get("options"), dict) else {}
    api_key = normalize_text(options.get("apiKey"))
    base_url = _normalize_base_url(options.get("baseURL"))
    if not api_key or not base_url:
        return {}
    return {
        "provider": "gmn",
        "base_url": base_url,
        "api_key": api_key,
        "credential_source": _pretty_home_path(path),
    }


def _resolve_model_candidates_from_opencode(path: Path) -> list[str]:
    data = _load_json_file(path)
    provider = data.get("provider") if isinstance(data.get("provider"), dict) else {}
    openai_cfg = provider.get("openai") if isinstance(provider, dict) and isinstance(provider.get("openai"), dict) else {}
    models = openai_cfg.get("models") if isinstance(openai_cfg.get("models"), dict) else {}
    configured_model = normalize_text(data.get("model"))
    if "/" in configured_model:
        configured_model = configured_model.split("/", 1)[1]
    return _dedupe_nonempty_strings([configured_model, *models.keys()])


def _resolve_from_openclaw(path: Path) -> dict[str, object]:
    data = _load_json_file(path)
    models = data.get("models") if isinstance(data.get("models"), dict) else {}
    providers = models.get("providers") if isinstance(models.get("providers"), dict) else {}
    gmn = providers.get("gmn") if isinstance(providers.get("gmn"), dict) else {}
    api_key = normalize_text(gmn.get("apiKey"))
    base_url = _normalize_base_url(gmn.get("baseUrl"))
    if not api_key or not base_url:
        return {}
    return {
        "provider": "gmn",
        "base_url": base_url,
        "api_key": api_key,
        "credential_source": _pretty_home_path(path),
    }


def _resolve_model_candidates_from_openclaw(path: Path) -> list[str]:
    data = _load_json_file(path)
    models = data.get("models") if isinstance(data.get("models"), dict) else {}
    providers = models.get("providers") if isinstance(models.get("providers"), dict) else {}
    gmn = providers.get("gmn") if isinstance(providers.get("gmn"), dict) else {}
    available = gmn.get("models") if isinstance(gmn.get("models"), list) else []
    agents = data.get("agents") if isinstance(data.get("agents"), dict) else {}
    defaults = agents.get("defaults") if isinstance(agents.get("defaults"), dict) else {}
    model_cfg = defaults.get("model") if isinstance(defaults.get("model"), dict) else {}

    candidates: list[object] = []
    primary = normalize_text(model_cfg.get("primary"))
    if "/" in primary:
        primary = primary.split("/", 1)[1]
    candidates.append(primary)

    fallbacks = model_cfg.get("fallbacks") if isinstance(model_cfg.get("fallbacks"), list) else []
    for fallback in fallbacks:
        fallback_text = normalize_text(fallback)
        if "/" in fallback_text:
            fallback_text = fallback_text.split("/", 1)[1]
        candidates.append(fallback_text)

    for item in available:
        if not isinstance(item, dict):
            continue
        candidates.append(item.get("id"))
        candidates.append(item.get("name"))

    return _dedupe_nonempty_strings(candidates)


def _discover_model_candidates() -> list[str]:
    candidates = _resolve_model_candidates_from_env()
    for path in (
        Path.home() / ".ccman" / "openclaw.json",
        Path.home() / ".ccman" / "codex.json",
        Path.home() / ".config" / "opencode" / "opencode.json",
        Path.home() / ".openclaw" / "openclaw.json",
    ):
        if not path.exists():
            continue
        if path.name == "opencode.json":
            candidates.extend(_resolve_model_candidates_from_opencode(path))
        elif path.name == "openclaw.json" and path.parent.name == ".openclaw":
            candidates.extend(_resolve_model_candidates_from_openclaw(path))
        else:
            candidates.extend(_resolve_model_candidates_from_ccman(path))
    return _dedupe_nonempty_strings(candidates)


def resolve_summary_runtime(cfg: dict[str, object] | None = None) -> dict[str, object]:
    cfg = cfg or load_intel_config()
    summary_cfg = normalize_summary_config(cfg.get("summary"))
    resolved = _resolve_from_env() or _resolve_from_app_secrets()

    if not resolved:
        for path in (
            Path.home() / ".ccman" / "openclaw.json",
            Path.home() / ".ccman" / "codex.json",
            Path.home() / ".config" / "opencode" / "opencode.json",
            Path.home() / ".openclaw" / "openclaw.json",
        ):
            if not path.exists():
                continue
            if path.name == "opencode.json":
                resolved = _resolve_from_opencode(path)
            elif path.name == "openclaw.json" and path.parent.name == ".openclaw":
                resolved = _resolve_from_openclaw(path)
            else:
                resolved = _resolve_from_ccman(path)
            if resolved:
                break

    model = normalize_summary_model(summary_cfg.get("model"))
    model_candidates = _dedupe_nonempty_strings(
        [model, *SUMMARY_MODEL_FALLBACKS, *_discover_model_candidates()]
    )
    if resolved:
        return {
            **resolved,
            "available": True,
            "model": model,
            "model_candidates": model_candidates,
            "mode": summary_cfg.get("mode"),
            "fallback": DEFAULT_SUMMARY_FALLBACK,
        }
    return {
        "available": False,
        "provider": "",
        "base_url": "",
        "api_key": "",
        "credential_source": "",
        "model": model,
        "model_candidates": model_candidates,
        "mode": summary_cfg.get("mode"),
        "fallback": DEFAULT_SUMMARY_FALLBACK,
    }


def build_summary_status(cfg: dict[str, object] | None = None) -> dict[str, object]:
    cfg = cfg or load_intel_config()
    summary_cfg = normalize_summary_config(cfg.get("summary"))
    runtime = resolve_summary_runtime(cfg)
    active_mode = "ai" if summary_cfg["mode"] == DEFAULT_SUMMARY_MODE and runtime.get("available") else "fallback"

    if summary_cfg["mode"] != DEFAULT_SUMMARY_MODE:
        status_text = "已切换为仅规则摘要。"
    elif runtime.get("available"):
        status_text = f"AI 摘要可用，当前将优先使用 {runtime.get('provider')}/{runtime.get('model')}。"
    else:
        status_text = "未检测到可用 AI 凭证，将自动回退到翻译 + 截断摘要。"

    return {
        "ok": True,
        "summary": {
            "mode": summary_cfg["mode"],
            "model": runtime.get("model"),
            "provider": runtime.get("provider"),
            "ai_available": bool(runtime.get("available")),
            "active_mode": active_mode,
            "credential_source": runtime.get("credential_source"),
            "model_candidates": runtime.get("model_candidates"),
            "fallback": DEFAULT_SUMMARY_FALLBACK,
            "status_text": status_text,
        },
    }


def _extract_response_text(payload: dict[str, object]) -> str:
    direct = normalize_text(payload.get("output_text"))
    if direct:
        return direct

    chunks: list[str] = []
    output = payload.get("output")
    if not isinstance(output, list):
        return ""
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            text = normalize_text(part.get("text") or part.get("output_text") or part.get("value"))
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def _extract_json_payload(text: str) -> dict[str, object]:
    cleaned = normalize_text(text)
    if not cleaned:
        return {}
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    for candidate in (cleaned, cleaned[cleaned.find("{") : cleaned.rfind("}") + 1] if "{" in cleaned and "}" in cleaned else ""):
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return {}


def _call_responses_api(prompt: str, runtime: dict[str, object], timeout: int = 20, model: str | None = None) -> dict[str, object]:
    body = json.dumps(
        {
            "model": model or runtime.get("model"),
            "input": prompt,
            "store": False,
            "max_output_tokens": 900,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{normalize_text(runtime.get('base_url')).rstrip('/')}/responses",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {normalize_text(runtime.get('api_key'))}",
            "User-Agent": "intel-monitor/20260326",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    return payload if isinstance(payload, dict) else {}


def _build_prompt(batch: list[dict[str, object]]) -> str:
    compact_items = []
    for item in batch:
        compact_items.append(
            {
                "key": item.get("_summary_key"),
                "source": item.get("source"),
                "author": item.get("author"),
                "text": short_summary_text(normalize_tweet_text(item.get("text")), max_len=180),
            }
        )

    return (
        "你是日报摘要助手。"
        "把每条内容压缩成适合 Telegram 推送的中文短摘要。"
        "保留核心事实：谁、动作、对象、结果。"
        "不要加推测，不要写空话，不要用 Markdown，不要编号。"
        "每条 summary 控制在 28 到 68 个中文字符内。"
        "只返回 JSON，格式必须是："
        '{"items":[{"key":"原样返回","summary":"中文摘要"}]}'
        "。输入：\n"
        + json.dumps(compact_items, ensure_ascii=False)
    )


def _summarize_batch(
    batch: list[dict[str, object]],
    runtime: dict[str, object],
    *,
    timeout: int = AI_SUMMARY_PRIMARY_TIMEOUT_SECONDS,
) -> dict[str, str]:
    prompt = _build_prompt(batch)
    last_error: Exception | None = None
    for model in runtime.get("model_candidates", [runtime.get("model")]):
        try:
            payload = _call_responses_api(prompt, runtime, timeout=timeout, model=normalize_text(model) or None)
            raw_text = _extract_response_text(payload)
            parsed = _extract_json_payload(raw_text)
            items = parsed.get("items") if isinstance(parsed.get("items"), list) else []
            result: dict[str, str] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                key = normalize_text(item.get("key"))
                summary = short_summary_text(normalize_text(item.get("summary")), max_len=110)
                if key and summary:
                    result[key] = summary
            if result:
                return result
            if len(batch) == 1:
                batch_key = normalize_text(batch[0].get("_summary_key"))
                raw_summary = short_summary_text(raw_text, max_len=110)
                if batch_key and raw_summary:
                    return {batch_key: raw_summary}
        except Exception as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return {}


def _fallback_summary(item: dict[str, object]) -> str:
    original_text = normalize_tweet_text(item.get("text"))
    translated = translate_text_to_chinese(original_text)
    return short_summary_text(translated or original_text, max_len=110)


def _chunk_items(items: list[dict[str, object]], batch_size: int) -> list[list[dict[str, object]]]:
    safe_batch_size = max(int(batch_size or 0), 1)
    return [items[index : index + safe_batch_size] for index in range(0, len(items), safe_batch_size)]


def _run_summary_batches(
    batches: list[list[dict[str, object]]],
    runtime: dict[str, object],
    *,
    timeout: int,
) -> tuple[dict[str, str], list[str]]:
    if not batches:
        return {}, []

    summaries: dict[str, str] = {}
    errors: list[str] = []
    worker_count = min(AI_SUMMARY_WORKERS, len(batches))
    with ThreadPoolExecutor(max_workers=max(worker_count, 1)) as executor:
        future_map = {
            executor.submit(_summarize_batch, batch, runtime, timeout=timeout): batch
            for batch in batches
        }
        for future in as_completed(future_map):
            try:
                summaries.update(future.result())
            except Exception as exc:
                message = normalize_text(exc)
                if message and message not in errors:
                    errors.append(message)
    return summaries, errors


def apply_digest_summaries(sections: dict[str, list[dict[str, object]]], cfg: dict[str, object] | None = None) -> dict[str, object]:
    cfg = cfg or load_intel_config()
    runtime = resolve_summary_runtime(cfg)
    items: list[dict[str, object]] = []

    for section_name, section_items in sections.items():
        for index, item in enumerate(section_items):
            item["original_text"] = normalize_tweet_text(item.get("text"))
            item["_summary_key"] = f"{section_name}:{index}"
            items.append(item)

    populate_display_text(items)

    if not items:
        return {
            "mode": "fallback",
            "provider": runtime.get("provider"),
            "model": runtime.get("model"),
            "used_ai": False,
            "ai_items": 0,
            "fallback_items": 0,
            "credential_source": runtime.get("credential_source"),
            "reason": "no_items",
        }

    requested_mode = normalize_summary_mode((cfg.get("summary") if isinstance(cfg.get("summary"), dict) else {}).get("mode"))
    ai_summaries: dict[str, str] = {}
    reason = ""
    target_items = items[:MAX_AI_SUMMARY_ITEMS]

    if requested_mode == DEFAULT_SUMMARY_MODE and runtime.get("available"):
        try:
            initial_batches = _chunk_items(target_items, AI_SUMMARY_BATCH_SIZE)
            ai_summaries, errors = _run_summary_batches(
                initial_batches,
                runtime,
                timeout=AI_SUMMARY_PRIMARY_TIMEOUT_SECONDS,
            )
            missing_items = [
                item
                for item in target_items
                if normalize_text(item.get("_summary_key")) not in ai_summaries
            ]
            if missing_items:
                retry_summaries, retry_errors = _run_summary_batches(
                    [[item] for item in missing_items],
                    runtime,
                    timeout=AI_SUMMARY_RETRY_TIMEOUT_SECONDS,
                )
                ai_summaries.update(retry_summaries)
                errors.extend(error for error in retry_errors if error not in errors)
            if errors and not reason:
                reason = errors[0]
            if not ai_summaries and not reason:
                reason = "ai_no_result"
        except Exception as exc:
            ai_summaries = {}
            reason = normalize_text(exc) or "ai_failed"
    elif requested_mode != DEFAULT_SUMMARY_MODE:
        reason = "fallback_only"
    else:
        reason = "ai_unavailable"

    ai_count = 0
    fallback_count = 0
    for item in items:
        summary_key = normalize_text(item.get("_summary_key"))
        summary_text = short_summary_text(normalize_text(ai_summaries.get(summary_key)), max_len=110)
        if summary_text:
            item["summary_text"] = summary_text
            item["summary_mode"] = "ai"
            ai_count += 1
            continue
        item["summary_text"] = _fallback_summary(item)
        item["summary_mode"] = "fallback"
        fallback_count += 1

    for item in items:
        item.pop("_summary_key", None)

    return {
        "mode": "ai" if ai_count > 0 else "fallback",
        "provider": runtime.get("provider"),
        "model": runtime.get("model"),
        "used_ai": ai_count > 0,
        "ai_items": ai_count,
        "fallback_items": fallback_count,
        "total_items": len(items),
        "target_items": len(target_items),
        "credential_source": runtime.get("credential_source"),
        "model_candidates": runtime.get("model_candidates"),
        "reason": reason or ("partial_fallback" if fallback_count and ai_count else ""),
    }
