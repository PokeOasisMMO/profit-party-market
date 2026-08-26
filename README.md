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
