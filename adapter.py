"""Forge Console platform adapter for Hermes.

Hermes initiates the connection to Forge, just like a messaging platform
adapter. Forge keeps a per-agent channel queue; this adapter polls it, hands
incoming text to Hermes, and posts Hermes replies back to Forge.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.session import SessionSource

try:
    from gateway.platform_registry import PlatformEntry
except Exception:
    PlatformEntry = None

logger = logging.getLogger(__name__)

_ENV_TO_EXTRA = {
    "FORGE_SERVER_URL": "server_url",
    "FORGE_PAIRING_CODE": "pairing_code",
    "FORGE_RUNTIME_NAME": "runtime_name",
    "FORGE_CHANNEL_URL": "channel_url",
    "FORGE_CHANNEL_TOKEN": "channel_token",
    "FORGE_HERMES_API_URL": "hermes_api_url",
    "FORGE_HERMES_API_KEY": "hermes_api_key",
}

_IMPORT_ENV = {key: os.getenv(key, "").strip() for key in _ENV_TO_EXTRA}

_HTTP_HEADERS = {
    "accept": "application/json",
    "user-agent": "Forge-Hermes/0.1 (+https://github.com/baomi-app/forge-hermes)",
}


class ForgePlatformAdapter(BasePlatformAdapter):
    """Hermes messaging adapter that pairs Hermes with Forge Console."""

    supports_async_delivery = True

    def __init__(self, config: PlatformConfig):
        _debug("adapter init")
        super().__init__(config, Platform("forge"))
        self.server_url = _config_value(config, "FORGE_SERVER_URL").rstrip("/")
        self.pairing_code = _config_value(config, "FORGE_PAIRING_CODE")
        self.channel_url = _config_value(config, "FORGE_CHANNEL_URL").rstrip("/")
        self.channel_token = _config_value(config, "FORGE_CHANNEL_TOKEN")
        self.runtime_name = _config_value(config, "FORGE_RUNTIME_NAME", "Hermes")
        self.hermes_api_url = _hermes_api_url(config)
        self.hermes_api_key = _hermes_api_key(config)
        _debug(
            "resolved config "
            f"server={_config_source(config, 'FORGE_SERVER_URL')} "
            f"pairing={_config_source(config, 'FORGE_PAIRING_CODE')}:{_short_secret_hash(self.pairing_code)} "
            f"channel={_config_source(config, 'FORGE_CHANNEL_URL')} "
            f"runtime={_config_source(config, 'FORGE_RUNTIME_NAME')}"
        )
        self.agent_id: Optional[str] = None
        self._poll_task: Optional[asyncio.Task] = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        _debug("adapter connect")
        logger.info("Starting Forge adapter")
        if not self.channel_url or not self.channel_token:
            if not self.server_url:
                raise RuntimeError("FORGE_SERVER_URL is required")
            if not self.pairing_code:
                raise RuntimeError("FORGE_PAIRING_CODE is required")
            await self._pair()

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        return True

    async def _pair(self) -> None:
        _debug(f"pairing request code_hash={_short_secret_hash(self.pairing_code)} server={self.server_url}")
        payload = {
            "pairingCode": self.pairing_code,
            "runtimeInstanceId": os.uname().nodename if hasattr(os, "uname") else "hermes",
            "name": self.runtime_name,
            "capabilities": [
                "sessions",
                "automations",
                "runs",
                "run_events",
                "model_config",
            ],
        }
        result = await asyncio.to_thread(
            _post_json,
            f"{self.server_url}/api/agent-registrations/connect",
            payload,
        )
        agent = result.get("agent") if isinstance(result, dict) else None
        if isinstance(agent, dict):
            self.agent_id = str(agent.get("id") or "")
        self.channel_url = str(result.get("channelUrl") or self.channel_url).rstrip("/")
        self.channel_token = str(result.get("channelToken") or self.channel_token)
        runtime_config = result.get("runtimeConfig") if isinstance(result, dict) else None
        if isinstance(runtime_config, dict):
            hermes_api_url = str(runtime_config.get("hermesApiUrl") or "").strip()
            hermes_api_key = str(runtime_config.get("hermesApiKey") or "").strip()
            if hermes_api_url:
                self.hermes_api_url = hermes_api_url.rstrip("/")
            if hermes_api_key:
                self.hermes_api_key = hermes_api_key
        if not self.channel_url or not self.channel_token:
            raise RuntimeError("Forge pairing did not return channelUrl/channelToken")
        _save_state(
            {
                "server_url": self.server_url,
                "channel_url": self.channel_url,
                "channel_token": self.channel_token,
                "runtime_name": self.runtime_name,
                "agent_id": self.agent_id or "",
                "hermes_api_url": self.hermes_api_url,
                "hermes_api_key": self.hermes_api_key,
            }
        )
        _debug(f"paired agent={self.agent_id or 'unknown'}")
        logger.info("Forge adapter paired with %s as %s", self.server_url, self.agent_id or "unknown agent")

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self.channel_url or not self.channel_token:
            return SendResult(success=False, error="Forge channel is not connected.", error_kind="not_connected")
        try:
            result = await asyncio.to_thread(
                _post_json,
                f"{self.channel_url}/runtime/messages",
                {
                    "chatId": chat_id,
                    "content": content,
                    "replyTo": reply_to,
                    "metadata": metadata or {},
                },
                self.channel_token,
            )
        except Exception as exc:
            return SendResult(success=False, error=str(exc), error_kind="network")
        message = result.get("message") if isinstance(result, dict) else None
        message_id = str(message.get("externalId")) if isinstance(message, dict) and message.get("externalId") else None
        return SendResult(success=True, message_id=message_id, raw_response=result)

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                item = await asyncio.to_thread(
                    _get_json,
                    f"{self.channel_url}/runtime/poll",
                    self.channel_token,
                )
                if not item:
                    await asyncio.sleep(1.5)
                    continue
                if item.get("kind") == "command":
                    await self._handle_command(item)
                    continue
                session_id = str(item.get("sessionId") or item.get("chatId") or "forge")
                text = str(item.get("text") or "")
                if not text:
                    continue
                event = MessageEvent(
                    text=text,
                    message_type=MessageType.TEXT,
                    source=SessionSource(
                        platform=Platform("forge"),
                        chat_id=session_id,
                        chat_name="Forge Console",
                        chat_type="dm",
                        user_id="forge-user",
                        user_name="Forge",
                        message_id=str(item.get("id") or ""),
                    ),
                    raw_message=item,
                    message_id=str(item.get("id") or ""),
                )
                await self.handle_message(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Forge poll loop failed")
                await asyncio.sleep(5)

    async def _handle_command(self, item: dict[str, Any]) -> None:
        command_id = str(item.get("id") or "")
        if not command_id:
            return
        try:
            _debug(f"command request id={command_id} method={item.get('method')} path={item.get('path')}")
            status, content_type, body = await asyncio.to_thread(
                _execute_hermes_command,
                self.hermes_api_url,
                self.hermes_api_key,
                str(item.get("method") or "GET"),
                str(item.get("path") or "/"),
                item.get("body") if isinstance(item.get("body"), str) else None,
            )
            _debug(f"command response id={command_id} status={status}")
        except Exception as exc:
            status = 502
            content_type = "application/json; charset=utf-8"
            body = json.dumps({"error": str(exc)})
            _debug(f"command failed id={command_id} error={exc}")
        await asyncio.to_thread(
            _post_json,
            f"{self.channel_url}/runtime/commands/{command_id}/response",
            {"status": status, "contentType": content_type, "body": body},
            self.channel_token,
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "id": chat_id,
            "name": "Forge Console",
            "type": "channel",
        }


def register(ctx) -> None:
    """Hermes plugin entry point."""
    _debug("register called")
    if not hasattr(ctx, "register_platform"):
        raise RuntimeError("Hermes plugin context does not expose register_platform")

    base_kwargs = {
        "name": "forge",
        "label": "Forge Console",
        "adapter_factory": lambda cfg: ForgePlatformAdapter(cfg),
    }
    entry_kwargs = _supported_platform_entry_kwargs(
        {
            "check_fn": check_requirements,
            "validate_config": validate_config,
            "required_env": [],
            "optional_env": [
                "FORGE_SERVER_URL",
                "FORGE_PAIRING_CODE",
                "FORGE_RUNTIME_NAME",
                "FORGE_CHANNEL_URL",
                "FORGE_CHANNEL_TOKEN",
                "FORGE_HERMES_API_URL",
                "FORGE_HERMES_API_KEY",
            ],
            "env_enablement_fn": _env_enablement,
            "apply_yaml_config_fn": _apply_yaml_config,
            "allow_all_env": "FORGE_ALLOW_ALL_USERS",
            "max_message_length": 0,
            "platform_hint": "You are chatting through Forge Console. Markdown is supported.",
        }
    )
    try:
        ctx.register_platform(**base_kwargs, **entry_kwargs)
        _debug(f"register_platform succeeded keys={','.join(sorted(entry_kwargs))}")
    except TypeError as exc:
        _debug(f"register_platform type error: {exc}; retrying minimal")
        ctx.register_platform(
            **base_kwargs,
            check_fn=check_requirements,
            validate_config=validate_config,
            required_env=[],
            optional_env=[
                "FORGE_SERVER_URL",
                "FORGE_PAIRING_CODE",
                "FORGE_RUNTIME_NAME",
                "FORGE_CHANNEL_URL",
                "FORGE_CHANNEL_TOKEN",
                "FORGE_HERMES_API_URL",
                "FORGE_HERMES_API_KEY",
            ],
        )
        _debug("register_platform minimal succeeded")


def check_requirements() -> bool:
    _debug("check_requirements called")
    return True


def validate_config(config: PlatformConfig) -> bool:
    _debug("validate_config called")
    channel_url = _config_value(config, "FORGE_CHANNEL_URL")
    channel_token = _config_value(config, "FORGE_CHANNEL_TOKEN")
    if channel_url and channel_token:
        return True
    return bool(_config_value(config, "FORGE_SERVER_URL") and _config_value(config, "FORGE_PAIRING_CODE"))


def _env_enablement() -> Optional[dict[str, str]]:
    _debug("env_enablement called")
    seed = {}
    for env_key, extra_key in _ENV_TO_EXTRA.items():
        value = os.getenv(env_key, "").strip()
        if value:
            seed[extra_key] = value
    state = _load_state()
    for extra_key in _ENV_TO_EXTRA.values():
        value = state.get(extra_key)
        if value:
            seed.setdefault(extra_key, str(value))
    has_pairing = bool(seed.get("server_url") and seed.get("pairing_code"))
    has_channel = bool(seed.get("channel_url") and seed.get("channel_token"))
    if not (has_pairing or has_channel):
        _debug("env_enablement skipped")
        return None
    seed.setdefault("runtime_name", "Hermes")
    _debug("env_enablement enabled")
    return seed


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict[str, str]]:
    _debug("apply_yaml_config called")
    seed = {}
    for env_key, extra_key in _ENV_TO_EXTRA.items():
        existing_env = os.getenv(env_key, "").strip()
        value = _first_config_value(yaml_cfg, platform_cfg, env_key, extra_key)
        if value:
            text = str(value).strip()
            seed[extra_key] = text
            os.environ.setdefault(env_key, text)
            if env_key == "FORGE_PAIRING_CODE":
                _debug(
                    "apply_yaml_config pairing "
                    f"existing_env={_short_secret_hash(existing_env)} "
                    f"yaml={_short_secret_hash(text)} "
                    f"final_env={_short_secret_hash(os.getenv(env_key, ''))}"
                )
    return seed or None


def _supported_platform_entry_kwargs(entry_kwargs: dict[str, Any]) -> dict[str, Any]:
    if PlatformEntry is None or not dataclasses.is_dataclass(PlatformEntry):
        _debug("platform entry fields unavailable; using all register kwargs")
        return entry_kwargs
    fields = {field.name for field in dataclasses.fields(PlatformEntry)}
    filtered = {key: value for key, value in entry_kwargs.items() if key in fields}
    dropped = sorted(set(entry_kwargs) - set(filtered))
    if dropped:
        _debug(f"dropped unsupported register kwargs={','.join(dropped)}")
    return filtered


def _config_value(config: PlatformConfig, key: str, default: str = "") -> str:
    import_value = _IMPORT_ENV.get(key)
    if import_value:
        return import_value
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    extra = getattr(config, "extra", None)
    if isinstance(extra, dict):
        extra_key = _ENV_TO_EXTRA.get(key, key.lower())
        value = extra.get(key) or extra.get(key.lower()) or extra.get(extra_key)
        if value:
            return str(value)
    value = getattr(config, key.lower(), None) or getattr(config, _ENV_TO_EXTRA.get(key, key.lower()), None)
    if value:
        return str(value)
    state = _load_state()
    state_value = state.get(_ENV_TO_EXTRA.get(key, key.lower()))
    if state_value:
        return str(state_value)
    return default


def _config_source(config: PlatformConfig, key: str) -> str:
    if _IMPORT_ENV.get(key):
        return "import_env"
    if os.environ.get(key):
        return "env"
    extra = getattr(config, "extra", None)
    if isinstance(extra, dict):
        extra_key = _ENV_TO_EXTRA.get(key, key.lower())
        if extra.get(key) or extra.get(key.lower()) or extra.get(extra_key):
            return "extra"
    if getattr(config, key.lower(), None) or getattr(config, _ENV_TO_EXTRA.get(key, key.lower()), None):
        return "attribute"
    state = _load_state()
    if state.get(_ENV_TO_EXTRA.get(key, key.lower())):
        return "state"
    return "default"


def _first_config_value(yaml_cfg: dict, platform_cfg: dict, env_key: str, extra_key: str) -> Optional[Any]:
    candidates = [
        yaml_cfg.get(env_key),
        yaml_cfg.get(extra_key),
        platform_cfg.get(env_key) if isinstance(platform_cfg, dict) else None,
        platform_cfg.get(extra_key) if isinstance(platform_cfg, dict) else None,
    ]
    extra = platform_cfg.get("extra") if isinstance(platform_cfg, dict) else None
    if isinstance(extra, dict):
        candidates.extend([extra.get(env_key), extra.get(extra_key)])
    for value in candidates:
        if value:
            return value
    return None


def _hermes_api_url(config: PlatformConfig) -> str:
    for key in ["FORGE_HERMES_API_URL", "HERMES_API_URL", "API_SERVER_URL", "HERMES_ENDPOINT"]:
        value = _config_value(config, key)
        if value:
            return value.rstrip("/")
    return "http://127.0.0.1:8765"


def _hermes_api_key(config: PlatformConfig) -> str:
    for key in ["FORGE_HERMES_API_KEY", "HERMES_API_KEY", "API_SERVER_KEY", "HERMES_API_TOKEN"]:
        value = _config_value(config, key)
        if value:
            return value
    return ""


def _state_path() -> Path:
    home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")
    return home / "forge-channel.json"


def _load_state() -> dict[str, Any]:
    try:
        path = _state_path()
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        _debug(f"state load failed error={exc}")
        return {}


def _save_state(state: dict[str, Any]) -> None:
    try:
        path = _state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        path.chmod(0o600)
        _debug(f"state saved path={path}")
    except Exception as exc:
        _debug(f"state save failed error={exc}")


def _debug(message: str) -> None:
    if os.getenv("FORGE_DEBUG", "").lower() not in {"1", "true", "yes", "on"}:
        return
    try:
        home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")
        path = home / "forge-plugin-debug.log"
        timestamp = datetime.now(timezone.utc).isoformat()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{timestamp} {message}\n")
    except Exception:
        pass


def _short_secret_hash(value: str) -> str:
    if not value:
        return "empty"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


_debug("module imported")
_debug(
    "import env "
    f"server={bool(_IMPORT_ENV.get('FORGE_SERVER_URL'))} "
    f"pairing={_short_secret_hash(_IMPORT_ENV.get('FORGE_PAIRING_CODE', ''))} "
    f"channel={bool(_IMPORT_ENV.get('FORGE_CHANNEL_URL'))}"
)


def _post_json(url: str, payload: dict[str, Any], token: str = "") -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {**_HTTP_HEADERS, "content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Forge pairing failed ({error.code}): {detail}") from error
    return json.loads(text) if text else {}


def _execute_hermes_command(base_url: str, api_key: str, method: str, path: str, body: Optional[str]) -> tuple[int, str, str]:
    hermes_path = _hermes_api_path(path)
    headers = {**_HTTP_HEADERS}
    data = None
    if body is not None:
        data = body.encode("utf-8")
        headers["content-type"] = "application/json"
    if api_key:
        headers["authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        f"{base_url}{hermes_path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=55) as response:
            text = response.read().decode("utf-8")
            return response.status, response.headers.get("content-type", "application/json; charset=utf-8"), text
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        return error.code, error.headers.get("content-type", "application/json; charset=utf-8"), detail


def _hermes_api_path(path: str) -> str:
    if path == "/health" or path.startswith("/health?"):
        return path
    if path == "/capabilities" or path.startswith("/capabilities?"):
        return path.replace("/capabilities", "/v1/capabilities", 1)
    if path == "/sessions" or path.startswith("/sessions?") or path.startswith("/sessions/"):
        return f"/api{path}"
    if path == "/automations" or path.startswith("/automations?"):
        return path.replace("/automations", "/api/jobs", 1)
    if path.startswith("/automations/"):
        return path.replace("/automations/", "/api/jobs/", 1)
    if path == "/runs" or path.startswith("/runs?") or path.startswith("/runs/"):
        return f"/v1{path}"
    raise RuntimeError(f"Unsupported Forge channel command path: {path}")


def _get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={**_HTTP_HEADERS, "authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=35) as response:
            if response.status == 204:
                return {}
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        if error.code == 204:
            return {}
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Forge poll failed ({error.code}): {detail}") from error
    return json.loads(text) if text else {}
