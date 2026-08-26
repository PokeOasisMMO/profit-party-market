# Profit Party hosted market service

This is the cloud, suggestion-only Profit Party market service. It combines a read-only Topstep NQ feed with Databento and Alpaca context, and it never sends or executes orders.

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/PokeOasisMMO/profit-party-market)

## Three-step deployment

1. Click **Deploy to Render** above.
2. Enter these private values when Render asks:
   - `DATABENTO_API_KEY`
   - `ALPACA_API_KEY_ID`
   - `ALPACA_API_SECRET_KEY`
   - `TOPSTEP_USERNAME`
   - `TOPSTEP_API_KEY`
3. Approve the Blueprint and wait for the health check to turn green.

Never commit or upload a `.env` file. Render stores the Topstep login values as private environment variables. Topstep is restricted to contract discovery, historical bars, and the ProjectX market-data hub for quotes, tape, and DOM. The service contains no account or trade-execution routes. Its public WebSocket at `/ws` exposes read-only market snapshots so Profit Party can connect automatically; `/` shows service status and `/health` provides detailed provider health.

## Koda Discord bot

The same hosted service can keep Koda connected to the Profit Party Discord. Add these Render environment variables:

- `DISCORD_BOT_TOKEN` — secret bot token from the Discord Developer Portal
- `DISCORD_GUILD_ID=1415914999926358068`
- `DISCORD_VIP_ROLE_ID=1416508522920804433`
- `DISCORD_SITE_URL=https://profitparty.online`
- `DISCORD_NEWS_ENABLED=true`
- `DISCORD_NEWS_CHANNEL_NAME=newsfeed`
- `NQ_NEWS_SYMBOLS=QQQ,META,NVDA,MSFT,AAPL,AMZN,GOOGL,GOOG,AVGO,TSLA`

Koda registers guild slash commands on startup: `/nq`, `/setup`, `/levels`, `/flow`, `/session`, `/koda`, `/stats`, `/website`, `/news`, `/daily`, `/weekly`, and `/meta`. Every command verifies the invoking member has the configured VIP role. The bot only reads the existing hosted market snapshot and never sends orders.

The Discord newsroom automatically finds `#newsfeed` (or `#news-feed`, `#nq-news`, or `#market-news`) and posts source-linked NQ/META updates. It uses Alpaca's real-time news WebSocket with the existing Alpaca credentials, plus official Federal Reserve monetary-policy and BLS jobs/CPI/PPI RSS feeds. Koda adds the live NQ price response and a confirmation-based suggestion, posts meaningful bias/VWAP/META alerts, and schedules morning, opening, midday, closing, and Friday weekly briefs. It never invents a headline and never executes a trade.
