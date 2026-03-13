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

## Strategy Set (Theory-Based Seeds)

Default strategy seeds are now loaded from:

- `StrategyModule/StrategyCatalog.py`

Current strategy groups:

- 0DTE ATM breakout continuation:
  - `otl_0dte_atm_breakout_buy`
  - `otl_0dte_atm_breakout_sell`
- 1DTE fakeout reversal:
  - `otl_1dte_fakeout_short`
  - `otl_1dte_fakeout_long`
- Max Pain regime flip and magnet:
  - `otf_maxpain_flip_bull`
  - `otf_maxpain_flip_bear`
  - `otf_maxpain_magnet_reversion`
- Sideway and value-area rotation:
  - `otl_sideway_insidebar_rotation`
  - `otl_sideway_val_to_vah_buy`
  - `otl_sideway_vah_to_val_sell`
- Higher-timeframe bias anchors:
  - `otl_weekly_above_mp_poc_bull`
  - `otl_monthly_70000_pivot`

Note:

- Seeds are inserted only when strategy memory is empty.
- If you want to reload new seeds, clear local memory files under `openfang_memory_evolution/data/`.

## Production Extension Points

- Replace `MarketDataModule.DataCollector` with real Binance/Kraken feeds
- Replace `LLMModule.LLMManager` with real OpenAI/LLM provider calls
- Replace `ExecutionModule.APIHandler` with real exchange order client
- Add risk controls: position sizing, stop-loss, max drawdown, exposure limits
