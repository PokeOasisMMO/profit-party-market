from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


GATEWAY_DIR = Path(__file__).resolve().parent
load_dotenv(GATEWAY_DIR / ".env", override=False)

# A broad but still compact NQ context basket.  These are streamed from the
# user's existing Alpaca IEX connection.  Keeping the union here means older
# .env files that only list QQQ,NVDA automatically receive the upgraded brain.
ALPACA_NQ_CONTEXT_SYMBOLS = (
    "QQQ",
    "NVDA",
    "MSFT",
    "AAPL",
    "AMZN",
    "META",
    "GOOGL",
    "AVGO",
    "AMD",
    "TSLA",
    "SMH",
    "SOXX",
    "XLK",
    "IGV",
    "SPY",
    "IWM",
    "TLT",
    "IEF",
    "SHY",
)


def _csv(name: str, default: str) -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(value.strip() for value in raw.split(",") if value.strip())


def _integer(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _boolean(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    host: str
    port: int
    stale_after_ms: int
    broadcast_interval_ms: int
    allowed_origins: tuple[str, ...]
    suggestion_only: bool
    hosted_access_token: str | None
    databento_enabled: bool
    databento_api_key: str | None
    databento_dataset: str
    databento_symbol: str
    databento_stype: str
    databento_replay_minutes: int
    alpaca_api_key_id: str | None
    alpaca_api_secret_key: str | None
    alpaca_feed: str
    alpaca_symbols: tuple[str, ...]
    treasury_yields_enabled: bool
    treasury_poll_minutes: int
    cftc_positioning_enabled: bool
    cftc_poll_minutes: int
    @classmethod
    def from_environment(cls) -> "GatewayConfig":
        configured_alpaca_symbols = _csv("ALPACA_SYMBOLS", "QQQ,NVDA")
        alpaca_symbols = tuple(dict.fromkeys((*configured_alpaca_symbols, *ALPACA_NQ_CONTEXT_SYMBOLS)))
        return cls(
            host=os.getenv("GATEWAY_HOST", "0.0.0.0" if _boolean("SUGGESTION_ONLY", False) else "127.0.0.1"),
            port=_integer("GATEWAY_PORT", _integer("PORT", 8765)),
            stale_after_ms=_integer("STALE_AFTER_MS", 5_000),
            broadcast_interval_ms=_integer("BROADCAST_INTERVAL_MS", 250),
            allowed_origins=_csv(
                "ALLOWED_ORIGINS",
                "http://localhost:3000,http://localhost:4173,http://127.0.0.1:3000,http://127.0.0.1:4173,https://profit-party.shady0324.chatgpt.site,https://profitparty.online,https://www.profitparty.online",
            ),
            suggestion_only=True,
            hosted_access_token=os.getenv("HOSTED_ACCESS_TOKEN") or None,
            databento_enabled=_boolean("DATABENTO_ENABLED", False),
            databento_api_key=os.getenv("DATABENTO_API_KEY") or None,
            databento_dataset=os.getenv("DATABENTO_DATASET", "GLBX.MDP3"),
            databento_symbol=os.getenv("DATABENTO_SYMBOL", "NQ.v.0"),
            databento_stype=os.getenv("DATABENTO_STYPE", "continuous"),
            databento_replay_minutes=_integer("DATABENTO_REPLAY_MINUTES", 20),
            alpaca_api_key_id=os.getenv("ALPACA_API_KEY_ID") or None,
            alpaca_api_secret_key=os.getenv("ALPACA_API_SECRET_KEY") or None,
            alpaca_feed=os.getenv("ALPACA_FEED", "iex"),
            alpaca_symbols=alpaca_symbols,
            treasury_yields_enabled=_boolean("TREASURY_YIELDS_ENABLED", True),
            treasury_poll_minutes=max(30, _integer("TREASURY_POLL_MINUTES", 360)),
            cftc_positioning_enabled=_boolean("CFTC_POSITIONING_ENABLED", True),
            cftc_poll_minutes=max(60, _integer("CFTC_POLL_MINUTES", 360)),
        )

    def missing_credentials(self) -> list[str]:
        missing: list[str] = []
        if not self.databento_api_key:
            missing.append("DATABENTO_API_KEY")
        if not self.hosted_access_token:
            missing.append("HOSTED_ACCESS_TOKEN")
        return missing

    def missing_optional_credentials(self) -> list[str]:
        missing: list[str] = []
        if not self.alpaca_api_key_id:
            missing.append("ALPACA_API_KEY_ID")
        if not self.alpaca_api_secret_key:
            missing.append("ALPACA_API_SECRET_KEY")
        if self.databento_enabled and not self.databento_api_key:
            missing.append("DATABENTO_API_KEY")
        return missing


CONFIG = GatewayConfig.from_environment()
