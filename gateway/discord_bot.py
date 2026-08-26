from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands

from .config import GatewayConfig
from .discord_newsroom import KodaNewsroom, meta_embed, news_embed, period_brief_embed
from .koda_memory import KodaMemory
from .market_state import MarketState


BULL_COLOR = 0x00F59F
BEAR_COLOR = 0xFF4D57
NEUTRAL_COLOR = 0xFFC62D
OFFLINE_COLOR = 0x65736C
DISCORD_COLOR = 0x5865F2


class KodaDiscordBot(commands.Bot):
    """Profit Party's VIP-only, suggestion-only Discord bot."""

    def __init__(self, config: GatewayConfig, state: MarketState, memory: KodaMemory) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(command_prefix=commands.when_mentioned, intents=intents, help_command=None)
        self.config = config
        self.market_state = state
        self.koda_memory = memory
        self.commands_synced = False
        self.last_error: str | None = None
        self.newsroom = KodaNewsroom(self, config, state)

    async def setup_hook(self) -> None:
        await self.add_cog(KodaCommands(self))
        guild = discord.Object(id=int(self.config.discord_guild_id or 0))
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        self.commands_synced = True

    async def on_ready(self) -> None:
        await self.change_presence(
            status=discord.Status.online,
            activity=discord.Activity(type=discord.ActivityType.watching, name="NQ with Koda • /nq"),
        )
        await self.newsroom.start()

    async def close(self) -> None:
        await self.newsroom.stop()
        await super().close()

    async def on_error(self, event_method: str, *args: object, **kwargs: object) -> None:
        self.last_error = f"Discord event failed: {event_method}"

    def health(self) -> dict[str, object]:
        return {
            "configured": self.config.discord_bot_ready(),
            "connected": self.is_ready() and not self.is_closed(),
            "commandsSynced": self.commands_synced,
            "guildId": str(self.config.discord_guild_id) if self.config.discord_guild_id else None,
            "vipRoleId": str(self.config.discord_vip_role_id) if self.config.discord_vip_role_id else None,
            "user": str(self.user) if self.user else None,
            "latencyMs": round(self.latency * 1_000) if self.is_ready() else None,
            "error": self.last_error,
            "newsroom": self.newsroom.health(),
        }


class KodaCommands(commands.Cog):
    def __init__(self, bot: KodaDiscordBot) -> None:
        self.bot = bot

    async def _vip_snapshot(self, interaction: discord.Interaction) -> dict[str, Any] | None:
        if interaction.guild_id != self.bot.config.discord_guild_id:
            await interaction.response.send_message(
                "🔒 Koda commands only work inside the Profit Party server.",
                ephemeral=True,
            )
            return None

        if self.bot.config.discord_vip_role_id not in member_role_ids(interaction.user):
            await interaction.response.send_message(
                "🔒 This command is for members with the **𝒱𝐼𝒫** role.",
                ephemeral=True,
            )
            return None

        return await self.bot.market_state.snapshot()

    @app_commands.command(name="nq", description="Show Koda's live NQ market board")
    @app_commands.checks.cooldown(1, 4.0, key=lambda interaction: interaction.user.id)
    async def nq(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        instrument = snapshot.get("instrument") or {}
        metrics = snapshot.get("metrics") or {}
        levels = snapshot.get("levels") or {}
        bias = str(metrics.get("bias") or "NEUTRAL")
        embed = market_embed(snapshot, title="KODA • NQ LIVE BOARD")
        embed.add_field(name="Price", value=price(instrument.get("price")), inline=True)
        embed.add_field(name="Bias", value=bias_badge(bias), inline=True)
        embed.add_field(name="Confidence", value=percent(metrics.get("confidence")), inline=True)
        embed.add_field(name="VWAP", value=price(metrics.get("vwap")), inline=True)
        embed.add_field(name="ATR", value=number(metrics.get("atr"), 2), inline=True)
        embed.add_field(name="Trigger", value=price(levels.get("trigger")), inline=True)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="setup", description="Show Koda's current suggestion-only NQ plan")
    @app_commands.checks.cooldown(1, 6.0, key=lambda interaction: interaction.user.id)
    async def setup(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        metrics = snapshot.get("metrics") or {}
        levels = snapshot.get("levels") or {}
        bias = str(metrics.get("bias") or "NEUTRAL")
        embed = market_embed(snapshot, title="KODA • SUGGESTED NQ PLAN")
        embed.add_field(name="Direction", value=bias_badge(bias), inline=True)
        embed.add_field(name="Confidence", value=percent(metrics.get("confidence")), inline=True)
        embed.add_field(name="Status", value="✅ QUALIFIED" if snapshot.get("ready") else "⏳ WAIT", inline=True)
        if not snapshot.get("ready"):
            embed.description = "No clean setup is unlocked. Wait for fresh data and a confirmed market response."
        embed.add_field(name="Trigger", value=price(levels.get("trigger")), inline=True)
        embed.add_field(name="TP1", value=price(levels.get("tp1")), inline=True)
        embed.add_field(name="TP2", value=price(levels.get("tp2")), inline=True)
        embed.add_field(name="Stretch", value=price(levels.get("stretch")), inline=True)
        embed.add_field(name="Invalidation", value=price(levels.get("invalidation")), inline=True)
        embed.add_field(name="Expected Burst", value=points(levels.get("expectedBurst")), inline=True)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="levels", description="Show VWAP, liquidity, trigger, targets, and invalidation")
    @app_commands.checks.cooldown(1, 5.0, key=lambda interaction: interaction.user.id)
    async def levels(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        metrics = snapshot.get("metrics") or {}
        levels = snapshot.get("levels") or {}
        book = snapshot.get("orderBook") or {}
        embed = market_embed(snapshot, title="KODA • KEY NQ LEVELS")
        embed.add_field(name="VWAP", value=price(metrics.get("vwap")), inline=True)
        embed.add_field(name="Buy Liquidity", value=price(levels.get("buyLiquidity")), inline=True)
        embed.add_field(name="Sell Liquidity", value=price(levels.get("sellLiquidity")), inline=True)
        embed.add_field(name="Trigger", value=price(levels.get("trigger")), inline=True)
        embed.add_field(
            name="TP1 / TP2",
            value=f"{price(levels.get('tp1'))} / {price(levels.get('tp2'))}",
            inline=True,
        )
        embed.add_field(name="Invalidation", value=price(levels.get("invalidation")), inline=True)
        embed.add_field(
            name="Best Bid / Ask",
            value=f"{price(book.get('bestBid'))} / {price(book.get('bestAsk'))}",
            inline=False,
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="flow", description="Read NQ order flow, momentum, and liquidity pressure")
    @app_commands.checks.cooldown(1, 5.0, key=lambda interaction: interaction.user.id)
    async def flow(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        metrics = snapshot.get("metrics") or {}
        book = snapshot.get("orderBook") or {}
        embed = market_embed(snapshot, title="KODA • NQ FLOW READ")
        embed.add_field(name="Momentum", value=percent(metrics.get("momentum")), inline=True)
        embed.add_field(name="Order Flow", value=percent(metrics.get("orderFlow")), inline=True)
        embed.add_field(name="Liquidity", value=percent(metrics.get("liquidity")), inline=True)
        embed.add_field(name="Cumulative Delta", value=number(metrics.get("cumulativeDelta"), 0), inline=True)
        embed.add_field(name="Book Imbalance", value=signed_percent(book.get("imbalance")), inline=True)
        embed.add_field(name="Velocity", value=points(metrics.get("velocity")), inline=True)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="session", description="Show the active NQ session and feed condition")
    @app_commands.checks.cooldown(1, 5.0, key=lambda interaction: interaction.user.id)
    async def session(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        now_et = datetime.now(ZoneInfo("America/New_York"))
        session_name, session_note = nq_session(now_et)
        embed = market_embed(snapshot, title="KODA • SESSION CHECK")
        embed.description = f"**{session_name}**\n{session_note}"
        embed.add_field(name="New York Time", value=now_et.strftime("%I:%M:%S %p ET"), inline=True)
        embed.add_field(name="Feed", value=freshness(snapshot), inline=True)
        embed.add_field(
            name="NQ",
            value=price((snapshot.get("instrument") or {}).get("price")),
            inline=True,
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="koda", description="Ask for Koda's concise NQ read right now")
    @app_commands.checks.cooldown(1, 6.0, key=lambda interaction: interaction.user.id)
    async def koda(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        metrics = snapshot.get("metrics") or {}
        instrument = snapshot.get("instrument") or {}
        bias = str(metrics.get("bias") or "NEUTRAL")
        relation = vwap_relation(instrument.get("price"), metrics.get("vwap"))
        embed = market_embed(snapshot, title="KODA READ")
        if snapshot.get("ready"):
            embed.description = (
                f"**{bias_badge(bias)} at {percent(metrics.get('confidence'))} confidence.**\n"
                f"Price is {relation}. Momentum is {percent(metrics.get('momentum'))}; "
                f"order flow is {percent(metrics.get('orderFlow'))}."
            )
        else:
            embed.description = (
                f"**{bias_badge(bias)} — no qualified setup.**\n"
                f"Price is {relation}. Koda is waiting for a fresh, confirmed market response."
            )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="stats", description="Show Koda Memory's recent NQ setup results")
    @app_commands.checks.cooldown(1, 8.0, key=lambda interaction: interaction.user.id)
    async def stats(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        summary = self.bot.koda_memory.summary()
        embed = market_embed(snapshot, title="KODA MEMORY • RECENT SETUPS")
        embed.add_field(name="Tracked", value=str(summary.get("total", 0)), inline=True)
        embed.add_field(name="Resolved", value=str(summary.get("resolved", 0)), inline=True)
        embed.add_field(name="Win Rate", value=percent(summary.get("winRate")), inline=True)
        embed.add_field(name="Watching", value=str(summary.get("watching", 0)), inline=True)
        setups = summary.get("setups") or []
        recent = [
            f"`{row.get('direction', '—')}` • {row.get('outcome', '—')} • {price(row.get('entryPrice'))}"
            for row in setups[:4]
        ]
        embed.add_field(
            name="Latest",
            value="\n".join(recent) if recent else "No completed observations yet.",
            inline=False,
        )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="website", description="Open the VIP Profit Party dashboard")
    @app_commands.checks.cooldown(1, 4.0, key=lambda interaction: interaction.user.id)
    async def website(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        view = discord.ui.View()
        view.add_item(discord.ui.Button(label="OPEN PROFIT PARTY", url=self.bot.config.discord_site_url, emoji="📈"))
        embed = discord.Embed(
            title="PROFIT PARTY • VIP ACCESS",
            description="Open Koda, TREE, live NQ intelligence, levels, and suggestion-only trade plans.",
            color=DISCORD_COLOR,
        )
        embed.set_footer(text="VIP role verified • Suggestions only")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="news", description="Show the latest NQ, META, or macro headline Koda received")
    @app_commands.checks.cooldown(1, 5.0, key=lambda interaction: interaction.user.id)
    async def news(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        articles = self.bot.newsroom.recent_articles
        if articles:
            embed = news_embed(articles[0], snapshot)
        else:
            status = self.bot.newsroom.health()
            embed = discord.Embed(
                title="KODA • NQ NEWSROOM",
                description="The newsroom is connected and waiting for the next relevant headline.",
                color=DISCORD_COLOR,
            )
            embed.add_field(
                name="Alpaca news",
                value="LIVE" if status["alpacaNewsConnected"] else "CONNECTING",
                inline=True,
            )
            embed.add_field(
                name="Official macro feeds",
                value=str(status["officialMacroFeeds"]),
                inline=True,
            )
            embed.set_footer(text="META • QQQ • Nasdaq leaders • Fed • Jobs • CPI • PPI")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="daily", description="Show Koda's rolling 24-hour NQ behavior brief")
    @app_commands.checks.cooldown(1, 8.0, key=lambda interaction: interaction.user.id)
    async def daily(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        await interaction.response.send_message(
            embed=period_brief_embed(snapshot, label="24H BEHAVIOR", hours=24)
        )

    @app_commands.command(name="weekly", description="Show Koda's rolling 7-day NQ behavior brief")
    @app_commands.checks.cooldown(1, 8.0, key=lambda interaction: interaction.user.id)
    async def weekly(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        await interaction.response.send_message(
            embed=period_brief_embed(snapshot, label="7D BEHAVIOR", hours=168)
        )

    @app_commands.command(name="meta", description="Show META's live Nasdaq impact and Koda's NQ read")
    @app_commands.checks.cooldown(1, 5.0, key=lambda interaction: interaction.user.id)
    async def meta(self, interaction: discord.Interaction) -> None:
        snapshot = await self._vip_snapshot(interaction)
        if snapshot is None:
            return
        await interaction.response.send_message(embed=meta_embed(snapshot))

    async def cog_app_command_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ) -> None:
        if isinstance(error, app_commands.CommandOnCooldown):
            message = f"⏱️ Koda is refreshing. Try again in {error.retry_after:.1f}s."
        else:
            self.bot.last_error = f"Command error: {type(error).__name__}"
            message = "⚠️ Koda hit a temporary error. Try the command again in a moment."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)


def member_role_ids(user: discord.User | discord.Member) -> set[int]:
    roles = getattr(user, "roles", [])
    return {int(role.id) for role in roles}


def market_embed(snapshot: dict[str, Any], *, title: str) -> discord.Embed:
    metrics = snapshot.get("metrics") or {}
    bias = str(metrics.get("bias") or "NEUTRAL")
    color = BULL_COLOR if bias == "BULLISH" else BEAR_COLOR if bias == "BEARISH" else NEUTRAL_COLOR
    if snapshot.get("stale"):
        color = OFFLINE_COLOR
    embed = discord.Embed(title=title, color=color, timestamp=datetime.now(UTC))
    embed.set_author(name="Profit Party • VIP Intelligence")
    embed.set_footer(text=f"{freshness(snapshot)} • Suggestions only • Never executes trades")
    return embed


def freshness(snapshot: dict[str, Any]) -> str:
    if snapshot.get("stale"):
        age_ms = snapshot.get("staleAgeMs")
        age = f"{float(age_ms) / 1_000:.1f}s old" if isinstance(age_ms, (int, float)) else "waiting"
        return f"STALE • {age}"
    return "LIVE"


def bias_badge(bias: str) -> str:
    return {"BULLISH": "🟢 BULLISH", "BEARISH": "🔴 BEARISH"}.get(bias, "🟡 NEUTRAL")


def price(value: object) -> str:
    return f"{float(value):,.2f}" if isinstance(value, (int, float)) else "—"


def number(value: object, digits: int = 2) -> str:
    return f"{float(value):,.{digits}f}" if isinstance(value, (int, float)) else "—"


def percent(value: object) -> str:
    return f"{float(value):.0f}%" if isinstance(value, (int, float)) else "—"


def signed_percent(value: object) -> str:
    return f"{float(value) * 100:+.1f}%" if isinstance(value, (int, float)) else "—"


def points(value: object) -> str:
    return f"{float(value):+.2f} pts" if isinstance(value, (int, float)) else "—"


def vwap_relation(last_price: object, vwap: object) -> str:
    if not isinstance(last_price, (int, float)) or not isinstance(vwap, (int, float)):
        return "waiting on VWAP"
    difference = float(last_price) - float(vwap)
    if abs(difference) < 0.25:
        return "sitting on VWAP"
    side = "above" if difference > 0 else "below"
    return f"{abs(difference):.2f} points {side} VWAP"


def nq_session(now_et: datetime) -> tuple[str, str]:
    minutes = now_et.hour * 60 + now_et.minute
    if minutes >= 18 * 60 or minutes < 2 * 60:
        return "ASIA SESSION", "Watch overnight range formation and avoid forcing thin liquidity."
    if minutes < 8 * 60 + 30:
        return "LONDON SESSION", "Watch the overnight range edges and the handoff into New York."
    if minutes < 11 * 60 + 30:
        return "NEW YORK OPEN", "Highest-impact NQ window. Let the opening response confirm direction."
    if minutes < 16 * 60:
        return "NEW YORK DAY", "Track VWAP acceptance, liquidity runs, and afternoon continuation."
    return "AFTER HOURS", "Liquidity can thin out. Protect size and wait for clean structure."


def create_discord_bot(
    config: GatewayConfig,
    state: MarketState,
    memory: KodaMemory,
) -> KodaDiscordBot | None:
    if not config.discord_bot_ready():
        return None
    return KodaDiscordBot(config, state, memory)
