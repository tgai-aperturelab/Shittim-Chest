"""Hermes reasoning-route predicates without global provider profiles or home configuration.

Derived from utils.py, transports/chat_completions.py and message_sanitization.py;
see third_party/hermes-replay-manifest.json. Only Chat Completions wire projection.
"""
from typing import Any
from urllib.parse import urlparse


def base_url_hostname(base_url: str) -> str:
    raw = (base_url or "").strip()
    try:
        parsed = urlparse(raw if "://" in raw else f"//{raw}") if raw else None
        return (parsed.hostname or "").lower().rstrip(".") if parsed else ""
    except ValueError:
        return ""


def base_url_host_matches(base_url: str, domain: str) -> bool:
    hostname = base_url_hostname(base_url)
    domain = (domain or "").strip().lower().rstrip(".")
    return bool(hostname and domain) and (hostname == domain or hostname.endswith("." + domain))


def model_consumes_thought_signature(model: Any, base_url: Any = None) -> bool:
    m = str(model or "").lower()
    return "gemini" in m or "gemma" in m or any(
        base_url_host_matches(base_url, host)
        for host in ("generativelanguage.googleapis.com", "aiplatform.googleapis.com")
    )


# Host-based rows from Hermes' route table. This application has no global provider profiles.
_CARRIER_ROUTES = (
    (frozenset(), ("mistral.ai", "groq.com", "cerebras.ai", "sambanova.ai", "hunyuan.cloud.tencent.com")),
    (frozenset({"reasoning_content"}), ("fireworks.ai",)),
    (frozenset({"reasoning_content", "reasoning", "reasoning_details"}),
     ("openrouter.ai", "kilo.ai", "ai-gateway.vercel.sh", "nousresearch.com")),
    (frozenset({"reasoning_content", "reasoning_details"}),
     ("novita.ai", "tokenhub.tencentmaas.com", "tokenhub-intl.tencentcloudmaas.com", "minimax.io", "minimaxi.com")),
)


def reasoning_carriers(base_url: str) -> frozenset[str]:
    return next((carriers for carriers, hosts in _CARRIER_ROUTES
                 if any(base_url_host_matches(base_url, host) for host in hosts)),
                frozenset({"reasoning_content"}))


def must_echo_reasoning(model: str, base_url: str) -> bool:
    return any(base_url_host_matches(base_url, host) for host in
               ("api.kimi.com", "moonshot.ai", "moonshot.cn", "api.deepseek.com", "api.xiaomimimo.com", "xiaomimimo.com")) or any(
                   family in model.lower() for family in ("deepseek", "mimo", "stealth/missingno")
               )
