# OpenFang Memory Evolution (SQLite + FAISS + LLM Prompt)

Project scaffold for:

- Market data ingestion -> feature extraction (RSI, MACD)
- Long-term memory in `SQLite`
- Vector retrieval via FAISS-compatible layer (default: pure Python cosine search)
- Feedback loop WIN/LOSS to adjust strategy weights
- Memory summaries + pruning over cycles
- Semantic ranking + BUY/SELL/HOLD decision

## Module Structure

```text
openfang_memory_evolution/
|-- MarketDataModule
|-- FeatureExtractionModule
|-- MemoryModule
|-- MemoryEvolutionModule
|-- SemanticRankingModule
|-- LLMModule
|-- DecisionMakingModule
|-- ExecutionModule
`-- app.py
```

## Quick Start (Windows one-click)

From repo root:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1 -RunDemo
```

Setup only (no demo run):

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1
```

Install optional FAISS backend:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1 -InstallFaiss
```

## Manual Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

Optional:

```bash
pip install faiss-cpu
```

2. Run demo:

```bash
python -m openfang_memory_evolution.app --symbol BTCUSDT --cycles 10
```

## Telegram Data Sync (continuous ingestion)

You can continuously ingest Telegram channel posts and store them in SQLite using:

- Bot API mode (requires bot token and channel access), or
- Public web scraping mode for public channels (`https://t.me/s/<channel>`) with no bot token.

Environment or CLI:

- Bot mode: `TELEGRAM_BOT_TOKEN`
- Public mode: `--telegram-web-sync --telegram-channel https://t.me/s/AI_otl_Alert`

Run sync-only worker (public channel, no bot token):

```bash
python -m openfang_memory_evolution.app --telegram-sync-only --telegram-web-sync --telegram-source-key otl_channel --telegram-channel https://t.me/s/AI_otl_Alert --telegram-poll-seconds 10
```

If your network injects a custom TLS certificate, add:

```bash
--telegram-web-insecure
```

Run trading cycles + sync before each cycle (public channel):

```bash
python -m openfang_memory_evolution.app --symbol BTCUSDT --cycles 20 --telegram-web-sync --telegram-source-key otl_channel --telegram-channel AI_otl_Alert
```

New SQLite tables:

- `telegram_sync_state`
- `telegram_message_history`
- `telegram_metric_history`

## Memory Evolution: Detailed Behavior

### 1) What happens after each trade cycle

In each call of `run_cycle` (`app.py`):

- Market data is collected and transformed to indicators/features.
- Similar strategies are retrieved from memory index.
- Strategies are ranked semantically (similarity + confidence + risk + historical weight).
- Decision is made: `BUY`, `SELL`, or `HOLD`.
- Trade record is always stored in SQLite (`trades` table), including:
  - `pnl`, `is_win`, `confidence`, `risk`, `reasoning`, `market_context_json`

### 2) Online learning trigger (immediate self-learning)

Feedback loop is applied only when:

- action is `BUY` or `SELL`, and
- a valid `strategy_id` exists

Then strategy stats are updated in SQLite (`strategies` table):

- If WIN (`pnl > 0`):
  - `wins += 1`
  - `weight += win_weight_boost` (default `+0.1`, capped at `3.0`)
- If LOSS (`pnl <= 0`):
  - `losses += 1`
  - `weight -= loss_weight_decay` (default `-0.1`, floored at `0.1`)

This means self-learning starts from the first BUY/SELL trade.

### 3) Batch evolution every 5 cycles

Every 5 cycles (`cycle_count % 5 == 0`), maintenance runs:

- `MemorySummaries.run()`:
  - rebuilds summary for each strategy:
  - `trades`, `win_rate`, `weight`, `active`
- `MemoryPruning.run()`:
  - checks bad-performing strategies and deactivates them
- `vector_index.save()`:
  - persists updated vector memory/index to disk

### 4) Pruning conditions

A strategy is considered for pruning only if:

- total trades (`wins + losses`) >= `min_trades_for_pruning` (default `5`)

Then:

- if `win_rate < min_win_rate_for_active` (default `0.4`):
  - strategy becomes `active = 0` in SQLite
  - strategy is removed from active vector index

Pruning here is soft-delete (deactivation), not hard-delete.

### 5) Time-based vs cycle-based behavior

Current implementation is cycle-based, not wall-clock based:

- Online feedback: per BUY/SELL trade, immediately
- Summary/pruning/index-sync: every 5 cycles

So you do not need to wait 24h for learning to start.

### 6) Dominant-expiration and anomaly-first logic

- Entry trigger is not fixed to `0DTE`.
- The engine detects which expiration/timeframe has the largest premium flow first.
- If abnormal bubble bursts appear in any timeframe (0DTE/weekly/monthly), that timeframe can become the trigger context.
- Trade confirmation is then checked against Max Pain/POC and price behavior.
- MP divergence reversion is scored directly:
  - if price is far from dominant Max Pain / MP cluster, reversion strategies are boosted
  - if price is near Max Pain, reversion edge is reduced

### 7) Bubble history storage for comparison

The system persists option bubble metrics to SQLite each cycle:

- `option_bubble_history`:
  - raw bubbles per event (`timeframe`, `expiry`, `strike`, `side`, `premium_usd`, `bubble_size`)
- `option_flow_snapshots`:
  - aggregated flow by timeframe/expiry per snapshot
  - includes `dominant_timeframe`, `dominant_expiry`, `anomaly_timeframe`, `anomaly_score`

This history is used to compare current flow versus recent baseline and compute anomaly scores.

## Strategy Set (Theory-Based Seeds)

Default strategy seeds are now loaded from:

- `StrategyModule/StrategyCatalog.py`

Current strategy groups:

- Any-timeframe anomaly breakout:
  - `otl_anytime_anomaly_breakout_buy`
  - `otl_anytime_anomaly_breakout_sell`
- Dominant-expiration flow first:
  - `otl_dominant_expiration_flow_buy`
  - `otl_dominant_expiration_flow_sell`
- 1DTE fakeout reversal:
  - `otl_1dte_fakeout_short`
  - `otl_1dte_fakeout_long`
- Max Pain regime flip and magnet:
  - `otf_maxpain_flip_bull`
  - `otf_maxpain_flip_bear`
  - `otf_maxpain_magnet_reversion`
- MP divergence reversion:
  - `otl_mp_divergence_reversion_sell`
  - `otl_mp_divergence_reversion_buy`
- Sideway and value-area rotation:
  - `otl_sideway_insidebar_rotation`
  - `otl_sideway_val_to_vah_buy`
  - `otl_sideway_vah_to_val_sell`
- Higher-timeframe bias anchors:
  - `otl_weekly_above_mp_poc_bull`
  - `otl_monthly_70000_pivot`

Note:

- Seeds are upserted on startup, so new strategy keys are added even on existing DB.
- To hard reset all strategy memory, clear local files under `openfang_memory_evolution/data/`.

## Production Extension Points

- Replace `MarketDataModule.DataCollector` with real Binance/Kraken feeds
- Replace `LLMModule.LLMManager` with real OpenAI/LLM provider calls
- Replace `ExecutionModule.APIHandler` with real exchange order client
- Add risk controls: position sizing, stop-loss, max drawdown, exposure limits
