"""Forge Console platform adapter for Hermes.

Hermes initiates the connection to Forge, just like a messaging platform
adapter. Forge keeps a per-agent channel queue; this adapter polls it, hands
incoming text to Hermes, and posts Hermes replies back to Forge.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

_ENV_TO_EXTRA = {
    "FORGE_SERVER_URL": "server_url",
    "FORGE_PAIRING_CODE": "pairing_code",
    "FORGE_RUNTIME_NAME": "runtime_name",
    "FORGE_CHANNEL_URL": "channel_url",
    "FORGE_CHANNEL_TOKEN": "channel_token",
}


class ForgePlatformAdapter(BasePlatformAdapter):
    """Hermes messaging adapter that pairs Hermes with Forge Console."""

    supports_async_delivery = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("forge"))
        self.server_url = _config_value(config, "FORGE_SERVER_URL").rstrip("/")
        self.pairing_code = _config_value(config, "FORGE_PAIRING_CODE")
        self.channel_url = _config_value(config, "FORGE_CHANNEL_URL").rstrip("/")
        self.channel_token = _config_value(config, "FORGE_CHANNEL_TOKEN")
        self.runtime_name = _config_value(config, "FORGE_RUNTIME_NAME", "Hermes")
        self.agent_id: Optional[str] = None
        self._poll_task: Optional[asyncio.Task] = None

    async def connect(self, *, is_reconnect: bool = False) -> bool:
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
        if not self.channel_url or not self.channel_token:
            raise RuntimeError("Forge pairing did not return channelUrl/channelToken")
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

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "id": chat_id,
            "name": "Forge Console",
            "type": "channel",
        }


def register(ctx) -> None:
    """Hermes plugin entry point."""
    ctx.register_platform(
        name="forge",
        label="Forge Console",
        adapter_factory=lambda cfg: ForgePlatformAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["FORGE_SERVER_URL", "FORGE_PAIRING_CODE"],
        optional_env=["FORGE_RUNTIME_NAME", "FORGE_CHANNEL_URL", "FORGE_CHANNEL_TOKEN"],
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        allow_all_env="FORGE_ALLOW_ALL_USERS",
        max_message_length=0,
        platform_hint="You are chatting through Forge Console. Markdown is supported.",
    )


def check_requirements() -> bool:
    return True


def validate_config(config: PlatformConfig) -> bool:
    channel_url = _config_value(config, "FORGE_CHANNEL_URL")
    channel_token = _config_value(config, "FORGE_CHANNEL_TOKEN")
    if channel_url and channel_token:
        return True
    return bool(_config_value(config, "FORGE_SERVER_URL") and _config_value(config, "FORGE_PAIRING_CODE"))


def _env_enablement() -> Optional[dict[str, str]]:
    seed = {}
    for env_key, extra_key in _ENV_TO_EXTRA.items():
        value = os.getenv(env_key, "").strip()
        if value:
            seed[extra_key] = value
    has_pairing = bool(seed.get("server_url") and seed.get("pairing_code"))
    has_channel = bool(seed.get("channel_url") and seed.get("channel_token"))
    if not (has_pairing or has_channel):
        return None
    seed.setdefault("runtime_name", "Hermes")
    return seed


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict[str, str]]:
    seed = {}
    for env_key, extra_key in _ENV_TO_EXTRA.items():
        value = _first_config_value(yaml_cfg, platform_cfg, env_key, extra_key)
        if value:
            text = str(value).strip()
            seed[extra_key] = text
            os.environ.setdefault(env_key, text)
    return seed or None


def _config_value(config: PlatformConfig, key: str, default: str = "") -> str:
    extra = getattr(config, "extra", None)
    if isinstance(extra, dict):
        extra_key = _ENV_TO_EXTRA.get(key, key.lower())
        value = extra.get(key) or extra.get(key.lower()) or extra.get(extra_key)
        if value:
            return str(value)
    value = getattr(config, key.lower(), None) or getattr(config, _ENV_TO_EXTRA.get(key, key.lower()), None)
    if value:
        return str(value)
    return os.environ.get(key, default)


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


def _post_json(url: str, payload: dict[str, Any], token: str = "") -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {"content-type": "application/json"}
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


def _get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"authorization": f"Bearer {token}"},
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
