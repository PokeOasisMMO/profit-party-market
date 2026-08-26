from __future__ import annotations

import asyncio
import html
import json
import math
import re
import time
import xml.etree.ElementTree as ET
from collections import deque
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from zoneinfo import ZoneInfo

import discord
import httpx
import websockets

from .config import GatewayConfig
from .market_state import MarketState


NEWS_COLOR = 0x5865F2
BULL_COLOR = 0x00F59F
BEAR_COLOR = 0xFF4D57
CAUTION_COLOR = 0xFFC62D
OFFLINE_COLOR = 0x65736C
NEW_YORK = ZoneInfo("America/New_York")

OFFICIAL_MACRO_FEEDS = (
    (
        "Federal Reserve",
        "MONETARY POLICY",
        "https://www.federalreserve.gov/feeds/press_monetary.xml",
    ),
    ("U.S. Bureau of Labor Statistics", "JOBS", "https://www.bls.gov/feed/empsit.rss"),
    ("U.S. Bureau of Labor Statistics", "CPI", "https://www.bls.gov/feed/cpi.rss"),
    ("U.S. Bureau of Labor Statistics", "PPI", "https://www.bls.gov/feed/ppi.rss"),
)

MACRO_KEYWORDS = (
    "federal reserve",
    "fed ",
    "fomc",
    "powell",
    "interest rate",
    "rate cut",
    "rate hike",
    "treasury yield",
    "inflation",
    "consumer price",
    "producer price",
    "cpi",
    "ppi",
    "payroll",
    "employment",
    "unemployment",
    "jobless",
    "jobs report",
    "tariff",
)

NASDAQ_KEYWORDS = (
    "nasdaq",
    "nasdaq-100",
    "nq futures",
    "big tech",
    "megacap",
    "semiconductor",
    "artificial intelligence",
    "ai chip",
)

BULLISH_WORDS = (
    "beats",
    "beat estimates",
    "raises guidance",
    "record revenue",
    "upgrade",
    "surges",
    "rallies",
    "jumps",
    "accelerates",
    "strong demand",
    "approval",
    "rate cut",
    "inflation cools",
    "jobs growth",
)

BEARISH_WORDS = (
    "misses",
    "cuts guidance",
    "downgrade",
    "falls",
    "drops",
    "plunges",
    "lawsuit",
    "antitrust",
    "investigation",
    "layoffs",
    "weak demand",
    "rate hike",
    "inflation rises",
    "hot inflation",
    "tariffs",
)


class KodaNewsroom:
    """Automatic NQ/META newsroom for Profit Party's Discord server."""

    def __init__(self, bot: discord.Client, config: GatewayConfig, state: MarketState) -> None:
        self.bot = bot
        self.config = config
        self.market_state = state
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(15, connect=8),
            headers={"User-Agent": "ProfitParty-Koda/1.0 (+https://profitparty.online)"},
            follow_redirects=True,
        )
        self.article_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=60)
        self.recent_articles: deque[dict[str, Any]] = deque(maxlen=20)
        self.seen_article_ids: deque[str] = deque(maxlen=1_000)
        self.seen_article_set: set[str] = set()
        self.rss_initialized: set[str] = set()
        self.sent_schedule_keys: set[str] = set()
        self.last_alert_at: dict[str, float] = {}
        self.last_bias: str | None = None
        self.last_vwap_side: str | None = None
        self.last_meta_band: int | None = None
        self.channel_name: str | None = None
        self.channel_found = False
        self.alpaca_connected = False
        self.last_news_at: str | None = None
        self.last_headline: str | None = None
        self.last_error: str | None = None

    async def start(self) -> None:
        if not self.config.discord_news_enabled or any(not task.done() for task in self.tasks):
            return
        self.tasks = [
            asyncio.create_task(self._news_stream_loop(), name="koda-alpaca-news"),
            asyncio.create_task(self._rss_poll_loop(), name="koda-official-macro-news"),
            asyncio.create_task(self._dispatch_loop(), name="koda-news-dispatch"),
            asyncio.create_task(self._scheduled_brief_loop(), name="koda-scheduled-briefs"),
            asyncio.create_task(self._market_watch_loop(), name="koda-market-watch"),
            asyncio.create_task(self._announce_startup(), name="koda-newsroom-startup"),
        ]

    async def stop(self) -> None:
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        await self.http.aclose()

    def health(self) -> dict[str, object]:
        return {
            "enabled": self.config.discord_news_enabled,
            "channel": self.channel_name or self.config.discord_news_channel_name,
            "channelFound": self.channel_found,
            "alpacaNewsConnected": self.alpaca_connected,
            "officialMacroFeeds": len(OFFICIAL_MACRO_FEEDS),
            "watchSymbols": list(self.config.nq_news_symbols),
            "lastNewsAt": self.last_news_at,
            "lastHeadline": self.last_headline,
            "queued": self.article_queue.qsize(),
            "error": self.last_error,
        }

    async def _resolve_channel(self) -> discord.TextChannel | None:
        guild = self.bot.get_guild(int(self.config.discord_guild_id or 0))
        if guild is None:
            self.channel_found = False
            self.last_error = "Profit Party Discord server is unavailable."
            return None

        channel: discord.TextChannel | None = None
        if self.config.discord_news_channel_id:
            candidate = guild.get_channel(self.config.discord_news_channel_id)
            if isinstance(candidate, discord.TextChannel):
                channel = candidate
        if channel is None:
            target = normalize_channel_name(self.config.discord_news_channel_name)
            aliases = {target, "newsfeed", "news-feed", "nq-news", "market-news"}
            channel = next(
                (
                    candidate
                    for candidate in guild.text_channels
                    if normalize_channel_name(candidate.name) in aliases
                ),
                None,
            )

        if channel is None:
            self.channel_found = False
            self.last_error = (
                f"Discord channel #{self.config.discord_news_channel_name} was not found."
            )
            return None

        member = guild.me
        permissions = channel.permissions_for(member) if member is not None else None
        if permissions is None or not (
            permissions.view_channel and permissions.send_messages and permissions.embed_links
        ):
            self.channel_found = False
            self.last_error = f"Koda cannot post embeds in #{channel.name}."
            return None

        self.channel_found = True
        self.channel_name = channel.name
        if self.last_error and ("channel" in self.last_error.lower() or "embeds" in self.last_error.lower()):
            self.last_error = None
        return channel

    async def _send_embed(self, embed: discord.Embed) -> bool:
        channel = await self._resolve_channel()
        if channel is None:
            return False
        try:
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            return True
        except discord.HTTPException as exc:
            self.last_error = f"Discord news post failed: {type(exc).__name__}"
            return False

    async def _announce_startup(self) -> None:
        await asyncio.sleep(3)
        embed = discord.Embed(
            title="🛰️ KODA NQ NEWSROOM • ONLINE",
            description=(
                "Koda is now watching **NQ**, **META**, QQQ, major Nasdaq drivers, "
                "Federal Reserve releases, jobs, CPI, and PPI."
            ),
            color=NEWS_COLOR,
            timestamp=datetime.now(UTC),
        )
        embed.add_field(
            name="Automatic updates",
            value="Breaking news • NQ behavior shifts • Daily brief • Weekly wrap",
            inline=False,
        )
        embed.add_field(
            name="Koda rule",
            value="News is context. Price response, trigger, and invalidation still control the suggestion.",
            inline=False,
        )
        embed.set_footer(text="Real sources • Live NQ context • Suggestions only")
        await self._send_embed(embed)

    async def _news_stream_loop(self) -> None:
        if not self.config.alpaca_api_key_id or not self.config.alpaca_api_secret_key:
            self.last_error = "Alpaca credentials are required for real-time news."
            return

        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_api_key_id,
            "APCA-API-SECRET-KEY": self.config.alpaca_api_secret_key,
        }
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    "wss://stream.data.alpaca.markets/v1beta1/news",
                    additional_headers=headers,
                    open_timeout=12,
                    ping_interval=20,
                    ping_timeout=12,
                    close_timeout=5,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    authenticated = False
                    for _ in range(4):
                        events = decode_events(await asyncio.wait_for(websocket.recv(), timeout=12))
                        if any(event.get("T") == "error" for event in events):
                            raise RuntimeError(str(events))
                        if any(
                            event.get("T") == "success"
                            and event.get("msg") == "authenticated"
                            for event in events
                        ):
                            authenticated = True
                            break
                    if not authenticated:
                        raise RuntimeError("Alpaca news authentication did not complete")

                    await websocket.send(json.dumps({"action": "subscribe", "news": ["*"]}))
                    self.alpaca_connected = True
                    self.last_error = None
                    backoff = 1.0
                    async for message in websocket:
                        for event in decode_events(message):
                            if event.get("T") == "error":
                                raise RuntimeError(str(event.get("msg") or event))
                            if event.get("T") == "n":
                                await self._enqueue_article(normalize_alpaca_article(event))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.alpaca_connected = False
                self.last_error = f"Alpaca news: {type(exc).__name__}: {exc}"

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=backoff)
            except TimeoutError:
                pass
            backoff = min(60.0, backoff * 2)

    async def _rss_poll_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.gather(
                *(self._poll_one_rss(source, category, url) for source, category, url in OFFICIAL_MACRO_FEEDS),
                return_exceptions=True,
            )
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=300)
            except TimeoutError:
                pass

    async def _poll_one_rss(self, source: str, category: str, url: str) -> None:
        try:
            response = await self.http.get(url)
            response.raise_for_status()
            articles = parse_rss_items(response.content, source=source, category=category)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = f"Official news feed: {type(exc).__name__}"
            return

        first_poll = url not in self.rss_initialized
        self.rss_initialized.add(url)
        for article in reversed(articles[:10]):
            article_id = str(article.get("id") or article.get("url") or article.get("headline"))
            if first_poll:
                published = parse_datetime(article.get("created_at"))
                if published is None or datetime.now(UTC) - published > timedelta(minutes=15):
                    self._remember_article_id(article_id)
                    continue
            await self._enqueue_article(article)

    async def _enqueue_article(self, article: dict[str, Any]) -> None:
        if not is_relevant_article(article, set(self.config.nq_news_symbols)):
            return
        article_id = str(article.get("id") or article.get("url") or article.get("headline"))
        if not article_id or article_id in self.seen_article_set:
            return
        self._remember_article_id(article_id)
        self.recent_articles.appendleft(article)
        if self.article_queue.full():
            try:
                self.article_queue.get_nowait()
                self.article_queue.task_done()
            except asyncio.QueueEmpty:
                pass
        await self.article_queue.put(article)

    def _remember_article_id(self, article_id: str) -> None:
        if not article_id or article_id in self.seen_article_set:
            return
        if len(self.seen_article_ids) == self.seen_article_ids.maxlen:
            expired = self.seen_article_ids.popleft()
            self.seen_article_set.discard(expired)
        self.seen_article_ids.append(article_id)
        self.seen_article_set.add(article_id)

    async def _dispatch_loop(self) -> None:
        while not self.stop_event.is_set():
            article = await self.article_queue.get()
            try:
                snapshot = await self.market_state.snapshot()
                if await self._send_embed(news_embed(article, snapshot)):
                    self.last_news_at = datetime.now(UTC).isoformat()
                    self.last_headline = str(article.get("headline") or "")[:180]
            finally:
                self.article_queue.task_done()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=45)
            except TimeoutError:
                pass

    async def _scheduled_brief_loop(self) -> None:
        schedule = {
            (8, 20): ("MORNING", 24),
            (9, 35): ("OPENING PULSE", 24),
            (12, 0): ("MIDDAY", 24),
            (16, 5): ("CLOSING WRAP", 24),
        }
        while not self.stop_event.is_set():
            now_et = datetime.now(NEW_YORK)
            if now_et.weekday() < 5:
                item = schedule.get((now_et.hour, now_et.minute))
                if item:
                    label, hours = item
                    key = f"{now_et.date()}:{label}"
                    if key not in self.sent_schedule_keys:
                        self.sent_schedule_keys.add(key)
                        snapshot = await self.market_state.snapshot()
                        await self._send_embed(period_brief_embed(snapshot, label=label, hours=hours))
                if now_et.weekday() == 4 and (now_et.hour, now_et.minute) == (16, 15):
                    key = f"{now_et.date()}:WEEKLY"
                    if key not in self.sent_schedule_keys:
                        self.sent_schedule_keys.add(key)
                        snapshot = await self.market_state.snapshot()
                        await self._send_embed(period_brief_embed(snapshot, label="WEEKLY WRAP", hours=168))

            if len(self.sent_schedule_keys) > 40:
                today = str(now_et.date())
                self.sent_schedule_keys = {
                    key for key in self.sent_schedule_keys if key.startswith(today)
                }
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=20)
            except TimeoutError:
                pass

    async def _market_watch_loop(self) -> None:
        while not self.stop_event.is_set():
            snapshot = await self.market_state.snapshot()
            metrics = snapshot.get("metrics") or {}
            instrument = snapshot.get("instrument") or {}
            equities = snapshot.get("equities") or {}
            price_value = finite(instrument.get("price"))
            vwap = finite(metrics.get("vwap"))
            bias = str(metrics.get("bias") or "NEUTRAL")
            confidence = finite(metrics.get("confidence")) or 0.0
            vwap_side = (
                "ABOVE" if price_value is not None and vwap is not None and price_value > vwap
                else "BELOW" if price_value is not None and vwap is not None and price_value < vwap
                else "ON"
            )
            meta = equities.get("META") or {}
            meta_change = finite(meta.get("changePct"))
            meta_band = int(math.copysign(math.floor(abs(meta_change or 0)), meta_change or 1))

            if self.last_bias is None:
                self.last_bias = bias
                self.last_vwap_side = vwap_side
                self.last_meta_band = meta_band
            else:
                if (
                    bias in {"BULLISH", "BEARISH"}
                    and bias != self.last_bias
                    and confidence >= 65
                    and snapshot.get("ready")
                ):
                    await self._send_market_alert(
                        "bias",
                        market_alert_embed(snapshot, f"NQ BIAS FLIPPED {bias}"),
                        cooldown_seconds=720,
                    )
                if (
                    vwap_side in {"ABOVE", "BELOW"}
                    and self.last_vwap_side in {"ABOVE", "BELOW"}
                    and vwap_side != self.last_vwap_side
                    and confidence >= 55
                ):
                    await self._send_market_alert(
                        "vwap",
                        market_alert_embed(snapshot, f"NQ RECLAIMED {vwap_side} VWAP"),
                        cooldown_seconds=600,
                    )
                if (
                    meta_change is not None
                    and abs(meta_change) >= 1.5
                    and meta_band != self.last_meta_band
                ):
                    await self._send_market_alert(
                        "meta",
                        meta_embed(snapshot, title="META IMPACT ALERT"),
                        cooldown_seconds=1_800,
                    )

                self.last_bias = bias
                self.last_vwap_side = vwap_side
                self.last_meta_band = meta_band

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=15)
            except TimeoutError:
                pass

    async def _send_market_alert(
        self,
        key: str,
        embed: discord.Embed,
        *,
        cooldown_seconds: int,
    ) -> None:
        now = time.monotonic()
        if now - self.last_alert_at.get(key, 0.0) < cooldown_seconds:
            return
        if await self._send_embed(embed):
            self.last_alert_at[key] = now


def decode_events(message: str | bytes) -> list[dict[str, Any]]:
    payload = json.loads(message)
    events = payload if isinstance(payload, list) else [payload]
    return [event for event in events if isinstance(event, dict)]


def normalize_alpaca_article(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"alpaca:{event.get('id')}",
        "headline": clean_text(event.get("headline")),
        "summary": clean_text(event.get("summary")),
        "source": clean_text(event.get("source")) or "Alpaca News",
        "url": str(event.get("url") or ""),
        "symbols": [str(symbol).upper() for symbol in event.get("symbols") or []],
        "created_at": str(event.get("created_at") or event.get("updated_at") or ""),
        "category": "MARKET NEWS",
    }


def parse_rss_items(content: bytes, *, source: str, category: str) -> list[dict[str, Any]]:
    root = ET.fromstring(content)
    result: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        headline = clean_text(item.findtext("title"))
        url = clean_text(item.findtext("link"))
        description = clean_text(item.findtext("description"))
        published = clean_text(item.findtext("pubDate")) or namespaced_text(item, "date")
        result.append(
            {
                "id": f"rss:{source}:{clean_text(item.findtext('guid')) or url or headline}",
                "headline": headline,
                "summary": description,
                "source": source,
                "url": url,
                "symbols": [],
                "created_at": normalize_published_at(published),
                "category": category,
                "macro": True,
            }
        )

    if result:
        return result

    for entry in root.findall(".//{*}entry"):
        link_node = entry.find("{*}link")
        url = str(link_node.get("href") or "") if link_node is not None else ""
        headline = clean_text(entry.findtext("{*}title"))
        result.append(
            {
                "id": f"rss:{source}:{entry.findtext('{*}id') or url or headline}",
                "headline": headline,
                "summary": clean_text(entry.findtext("{*}summary")),
                "source": source,
                "url": url,
                "symbols": [],
                "created_at": normalize_published_at(
                    entry.findtext("{*}published") or entry.findtext("{*}updated") or ""
                ),
                "category": category,
                "macro": True,
            }
        )
    return result


def namespaced_text(element: ET.Element, local_name: str) -> str:
    for child in element:
        if child.tag.rsplit("}", 1)[-1] == local_name and child.text:
            return clean_text(child.text)
    return ""


def normalize_published_at(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        parsed = parse_datetime(text)
    if parsed is None:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def is_relevant_article(article: dict[str, Any], watch_symbols: set[str]) -> bool:
    symbols = {str(symbol).upper() for symbol in article.get("symbols") or []}
    if symbols & watch_symbols:
        return True
    text = f"{article.get('headline', '')} {article.get('summary', '')}".lower()
    return bool(article.get("macro")) or any(
        keyword in text for keyword in (*MACRO_KEYWORDS, *NASDAQ_KEYWORDS)
    )


def classify_news(article: dict[str, Any]) -> str:
    if article.get("macro"):
        return "CAUTION"
    text = f"{article.get('headline', '')} {article.get('summary', '')}".lower()
    score = sum(1 for word in BULLISH_WORDS if word in text)
    score -= sum(1 for word in BEARISH_WORDS if word in text)
    if score > 0:
        return "BULLISH"
    if score < 0:
        return "BEARISH"
    return "CAUTION" if any(word in text for word in MACRO_KEYWORDS) else "NEUTRAL"


def news_embed(article: dict[str, Any], snapshot: dict[str, Any]) -> discord.Embed:
    direction = classify_news(article)
    color = BULL_COLOR if direction == "BULLISH" else BEAR_COLOR if direction == "BEARISH" else CAUTION_COLOR
    headline = str(article.get("headline") or "Market update")[:256]
    url = str(article.get("url") or "")
    embed = discord.Embed(
        title=headline,
        url=url if url.startswith(("https://", "http://")) else None,
        description=str(article.get("summary") or "No summary was supplied by the source.")[:1_200],
        color=color,
        timestamp=parse_datetime(article.get("created_at")) or datetime.now(UTC),
    )
    embed.set_author(
        name=f"{article.get('category') or 'NQ NEWS'} • {article.get('source') or 'Market source'}"
    )
    symbols = [str(symbol) for symbol in article.get("symbols") or []]
    metrics = snapshot.get("metrics") or {}
    instrument = snapshot.get("instrument") or {}
    embed.add_field(
        name="NQ live response",
        value=(
            f"{format_price(instrument.get('price'))} • {metrics.get('bias') or 'NEUTRAL'} • "
            f"{format_percent(metrics.get('confidence'))} confidence"
        ),
        inline=False,
    )
    embed.add_field(
        name="Koda suggestion",
        value=news_suggestion(article, snapshot),
        inline=False,
    )
    if symbols:
        embed.add_field(name="Related", value=" • ".join(symbols[:10]), inline=False)
    embed.set_footer(text="Source-linked news • Confirm with price • Suggestions only")
    return embed


def news_suggestion(article: dict[str, Any], snapshot: dict[str, Any]) -> str:
    if snapshot.get("stale"):
        return "NQ data is stale. Treat this as context only until the live feed recovers."
    direction = classify_news(article)
    metrics = snapshot.get("metrics") or {}
    levels = snapshot.get("levels") or {}
    bias = str(metrics.get("bias") or "NEUTRAL")
    if article.get("macro"):
        return "High-impact macro news: let NQ show its first response, then require VWAP acceptance before acting."
    if direction in {"BULLISH", "BEARISH"} and direction == bias and snapshot.get("ready"):
        return (
            f"News and price align {direction.lower()}. Wait for {format_price(levels.get('trigger'))}; "
            f"the idea is wrong beyond {format_price(levels.get('invalidation'))}."
        )
    if direction in {"BULLISH", "BEARISH"} and bias in {"BULLISH", "BEARISH"} and direction != bias:
        return "News and live NQ disagree. Do not chase the headline—wait for price to confirm or reject it."
    return "Context only right now. Use the live trigger, VWAP response, and invalidation before considering a setup."


def period_brief_embed(
    snapshot: dict[str, Any],
    *,
    label: str,
    hours: int,
) -> discord.Embed:
    stats = period_stats(snapshot, hours=hours)
    metrics = snapshot.get("metrics") or {}
    instrument = snapshot.get("instrument") or {}
    equities = snapshot.get("equities") or {}
    meta = equities.get("META") or {}
    bias = str(metrics.get("bias") or "NEUTRAL")
    color = BULL_COLOR if bias == "BULLISH" else BEAR_COLOR if bias == "BEARISH" else CAUTION_COLOR
    embed = discord.Embed(
        title=f"📊 KODA • NQ {label}",
        description=behavior_read(snapshot, stats),
        color=color,
        timestamp=datetime.now(UTC),
    )
    embed.add_field(name="NQ", value=format_price(instrument.get("price")), inline=True)
    embed.add_field(name="Bias", value=bias, inline=True)
    embed.add_field(name="Confidence", value=format_percent(metrics.get("confidence")), inline=True)
    if stats:
        embed.add_field(name="Period move", value=format_points(stats.get("change")), inline=True)
        embed.add_field(name="Range", value=format_points(stats.get("range"), signed=False), inline=True)
        embed.add_field(
            name="High / Low",
            value=f"{format_price(stats.get('high'))} / {format_price(stats.get('low'))}",
            inline=True,
        )
    embed.add_field(
        name="META driver",
        value=f"{format_price(meta.get('price'))} • {format_signed_percent(meta.get('changePct'))}",
        inline=True,
    )
    embed.add_field(name="VWAP", value=format_price(metrics.get("vwap")), inline=True)
    embed.add_field(name="ATR", value=format_points(metrics.get("atr"), signed=False), inline=True)
    embed.set_footer(text="Live NQ + 24h/7d structure • Suggestions only")
    return embed


def market_alert_embed(snapshot: dict[str, Any], title: str) -> discord.Embed:
    metrics = snapshot.get("metrics") or {}
    levels = snapshot.get("levels") or {}
    bias = str(metrics.get("bias") or "NEUTRAL")
    color = BULL_COLOR if bias == "BULLISH" else BEAR_COLOR if bias == "BEARISH" else CAUTION_COLOR
    embed = discord.Embed(title=f"⚡ {title}", color=color, timestamp=datetime.now(UTC))
    embed.add_field(name="Bias", value=bias, inline=True)
    embed.add_field(name="Confidence", value=format_percent(metrics.get("confidence")), inline=True)
    embed.add_field(name="VWAP", value=format_price(metrics.get("vwap")), inline=True)
    embed.add_field(name="Trigger", value=format_price(levels.get("trigger")), inline=True)
    embed.add_field(name="TP1", value=format_price(levels.get("tp1")), inline=True)
    embed.add_field(name="Invalidation", value=format_price(levels.get("invalidation")), inline=True)
    embed.set_footer(text="Wait for confirmation • Suggestions only • No order execution")
    return embed


def meta_embed(snapshot: dict[str, Any], *, title: str = "META • NASDAQ DRIVER") -> discord.Embed:
    equities = snapshot.get("equities") or {}
    meta = equities.get("META") or {}
    metrics = snapshot.get("metrics") or {}
    change = finite(meta.get("changePct"))
    color = BULL_COLOR if (change or 0) > 0 else BEAR_COLOR if (change or 0) < 0 else CAUTION_COLOR
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(UTC))
    embed.add_field(name="META", value=format_price(meta.get("price")), inline=True)
    embed.add_field(name="Session change", value=format_signed_percent(change), inline=True)
    embed.add_field(name="NQ bias", value=str(metrics.get("bias") or "NEUTRAL"), inline=True)
    if change is None:
        read = "Waiting for the live META quote."
    elif abs(change) >= 1.5:
        read = "META is moving enough to matter for Nasdaq sentiment. Confirm whether NQ accepts the same direction."
    else:
        read = "META is not producing an outsized Nasdaq impulse right now."
    embed.description = read
    embed.set_footer(text="Alpaca IEX context • Suggestions only")
    return embed


def period_stats(snapshot: dict[str, Any], *, hours: int, now: datetime | None = None) -> dict[str, float] | None:
    timeframes = snapshot.get("timeframes") or {}
    bars = list(timeframes.get("1h") or [])
    now_utc = (now or datetime.now(UTC)).astimezone(UTC)
    cutoff = now_utc - timedelta(hours=hours)
    selected = []
    for bar in bars:
        moment = parse_datetime(bar.get("ts"))
        if moment is not None and moment >= cutoff:
            selected.append(bar)
    if not selected:
        return None
    open_value = finite(selected[0].get("open"))
    close_value = finite(selected[-1].get("close"))
    highs = [value for value in (finite(bar.get("high")) for bar in selected) if value is not None]
    lows = [value for value in (finite(bar.get("low")) for bar in selected) if value is not None]
    if open_value is None or close_value is None or not highs or not lows:
        return None
    high = max(highs)
    low = min(lows)
    return {
        "open": open_value,
        "high": high,
        "low": low,
        "close": close_value,
        "change": close_value - open_value,
        "changePct": ((close_value / open_value) - 1) * 100 if open_value else 0.0,
        "range": high - low,
    }


def behavior_read(snapshot: dict[str, Any], stats: dict[str, float] | None) -> str:
    metrics = snapshot.get("metrics") or {}
    bias = str(metrics.get("bias") or "NEUTRAL")
    confidence = finite(metrics.get("confidence")) or 0.0
    if snapshot.get("stale"):
        return "The NQ feed is stale, so Koda is withholding directional suggestions."
    if stats is None:
        return f"NQ is {bias.lower()} with {confidence:.0f}% confidence; longer-period structure is still loading."
    change = stats["change"]
    direction = "higher" if change > 0 else "lower" if change < 0 else "flat"
    return (
        f"NQ has traveled **{abs(change):,.2f} points {direction}** across this window with a "
        f"**{stats['range']:,.2f}-point range**. Koda currently reads **{bias}** at "
        f"**{confidence:.0f}% confidence**."
    )


def parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def clean_text(value: object) -> str:
    raw = html.unescape(str(value or ""))
    without_tags = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", without_tags).strip()


def normalize_channel_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def finite(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def format_price(value: object) -> str:
    number = finite(value)
    return f"{number:,.2f}" if number is not None else "—"


def format_percent(value: object) -> str:
    number = finite(value)
    return f"{number:.0f}%" if number is not None else "—"


def format_signed_percent(value: object) -> str:
    number = finite(value)
    return f"{number:+.2f}%" if number is not None else "—"


def format_points(value: object, *, signed: bool = True) -> str:
    number = finite(value)
    if number is None:
        return "—"
    return f"{number:+,.2f} pts" if signed else f"{number:,.2f} pts"
