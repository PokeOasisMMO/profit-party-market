# Profit Party hosted market service

This repository contains the hosted, suggestion-only Profit Party market service.

## Deploy on Render

1. Create a new GitHub repository named `profit-party-market`.
2. Upload every file and folder from this package to the repository root.
3. In Render, choose **New > Blueprint** and select that repository.
4. Render will detect `render.yaml` and create the `profit-party-market` web service.
5. When Render asks for secrets, enter:
   - `DATABENTO_API_KEY`
   - `HOSTED_ACCESS_TOKEN` (make this a long random private value)
   - `ALPACA_API_KEY_ID`
   - `ALPACA_API_SECRET_KEY`
6. Deploy and wait for the `/health` check to become green.

Do not upload a `.env` file. This service does not execute trades and does not use Topstep credentials. The live WebSocket is locked with `HOSTED_ACCESS_TOKEN`; never publish that value in this repository.
