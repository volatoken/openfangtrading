# Metrics Formula Mapping

This document is the implementation reference for `config/panels.yml`.
Each formula ID below maps 1:1 to `formula_ref` keys in YAML.

## 1) Input Conventions

- `symbol` (options): `BTC-YYMMDD-STRIKE-C|P`
- `cp`:
  - `C` -> call
  - `P` -> put
- `side_sign`:
  - BUY -> `+1`
  - SELL -> `-1`
- `qty`: contracts
- `premium_usdt`:
  - prefer `abs(quoteQty)` when present
  - else `abs(price * qty)`
- `S`: BTC index price from options index stream (fallback UM mark/index)
- `unit`: contract unit from `/eapi/v1/exchangeInfo`

## 2) Core Helpers

## parse_option_symbol

Purpose: normalize symbol into structural keys.

Pseudo:
```text
split = symbol.split("-")
underlying = split[0]            # BTC
expiry_yymmdd = split[1]         # 250327
strike = float(split[2])         # 70000
cp = split[3]                    # C/P
```

## expiry_group_classifier

Purpose: map each expiry into `odte`, `d1`, `weekly`, `monthly`, `all`.

Pseudo:
```text
if expiry_date == today_utc: odte
elif expiry_date == today_utc + 1 day: d1
elif is_next_weekly(expiry_date): weekly
elif is_monthly_cycle(expiry_date): monthly
else: all
```

## 3) Panel Metrics

## premium_by_strike

Used by: `pa5/pa6/pa7`, maxpain board overlays, strike analytics.

Definition:
```text
premium_by_strike = SUM(premium_usdt) grouped by [bucket_time, expiry_group, strike, cp]
premium_buy = SUM(premium_usdt where side_sign = +1)
premium_sell = SUM(premium_usdt where side_sign = -1)
```

SQL sketch:
```sql
SELECT
  date_trunc('minute', event_time) AS bucket_time,
  expiry_group,
  strike,
  cp,
  SUM(premium_usdt) AS premium_total,
  SUM(CASE WHEN side_sign = 1 THEN premium_usdt ELSE 0 END) AS premium_buy,
  SUM(CASE WHEN side_sign = -1 THEN premium_usdt ELSE 0 END) AS premium_sell
FROM option_trade_events
GROUP BY 1,2,3,4;
```

## volume_by_strike

Used by: `pa1`.

Definition:
```text
volume_by_strike = SUM(abs(qty)) grouped by [strike, cp]
```

## volume_by_expiry_24h

Used by: `pa2`.

Definition:
```text
volume_by_expiry_24h = SUM(volume_contracts) from latest option_ticker_24h snapshot by expiry
```

## premium_by_expiry_24h

Used by: `pa2`, dominant expiry scoring.

Definition:
```text
premium_by_expiry_24h = SUM(amount_usdt) by expiry
```

## trades_by_expiry_24h

Used by: `pa2`.

Definition:
```text
trades_by_expiry_24h = SUM(trade_count) by expiry
```

## oi_by_expiry

Used by: `pa3`, dominant expiry scoring.

Definition:
```text
oi_by_expiry = SUM(oi_contracts) by expiry
oi_usdt_by_expiry = SUM(oi_usdt) by expiry
```

## dominant_expiry_score

Used by: monthly/weekly panel priority and ODTE card.

Definition:
```text
score = w1 * zscore(premium_by_expiry_24h)
      + w2 * zscore(oi_by_expiry)
      + w3 * zscore(trades_by_expiry_24h)
default weights: w1=0.5, w2=0.35, w3=0.15
dominant_expiry = argmax(score)
```

## net_delta_flow_by_strike (inferred metric)

Used by: `pa10`, `pa5/6/7`.

Definition:
```text
net_delta_flow = SUM(side_sign * qty * delta * unit) by strike
net_delta_flow_usd = net_delta_flow * S
```

Notes:
- `delta` comes from options mark snapshot closest to trade timestamp.
- For put options delta is usually negative already; no manual sign flip needed.

## gex_flow_by_strike (inferred metric)

Used by: optional flow view in strike analytics.

Definition:
```text
gex_flow = SUM(side_sign * qty * gamma * unit * S^2 * 0.01) by strike
```

Unit:
- estimated USD change for 1 percent move in underlying.

## gex_oi_by_strike (inferred metric)

Used by: `pa11`, `pa5/6/7` (default mode).

Definition:
```text
gex_oi = SUM(oi_contracts * gamma * unit * S^2 * 0.01) by strike
```

Optional dealer-sign mode:
```text
gex_oi_signed = dealer_sign * gex_oi
dealer_sign from rolling flow bias if needed
```

## iv_smile_curve

Used by: `pa12.1`, `pa12.2`, `pa12.3`.

Definition:
```text
for selected expiry_group:
  x = strike
  y_call = median(markIV where cp='C')
  y_put  = median(markIV where cp='P')
```

Render:
- line for call IV
- line for put IV
- optional merged smile by delta bucket.

## iv_smile_curve_all_exp

Used by: `pa12.4`.

Definition:
```text
weighted_iv(strike) = SUM(markIV * weight_expiry) / SUM(weight_expiry)
weight_expiry default = premium_by_expiry_24h normalized
```

## atm_iv

Used by: IV panels and ODTE info.

Definition:
```text
atm_strike = strike with minimal abs(strike - S)
atm_iv = average(markIV at atm_strike across C and P)
```

## iv_skew

Used by: IV panels.

Definition:
```text
iv_skew = iv_put_otm(delta~0.25 abs) - iv_call_otm(delta~0.25 abs)
```

## max_pain_by_expiry (inferred metric)

Used by: P1/P2/P3/P4 panels.

Definition:
```text
For each candidate settlement strike K:
  payout_calls = SUM( oi_call_i * max(0, K - strike_i) * unit )
  payout_puts  = SUM( oi_put_i  * max(0, strike_i - K) * unit )
  total_payout(K) = payout_calls + payout_puts
max_pain = K with minimal total_payout(K)
```

Computation cadence:
- every 60 seconds (aligned with OI updates).

## poc_by_premium (inferred metric)

Used by: P1/P2/P3/P4 panels.

Definition:
```text
poc = strike with MAX(SUM(premium_usdt)) in lookback window (default 24h)
```

## btc_index

Used by: header.

Definition:
```text
btc_index = latest indexPrice from options index stream
fallback order: UM premiumIndex.indexPrice -> Spot lastPrice
```

## options_vol_24h_usdt

Used by: header.

Definition:
```text
options_vol_24h_usdt = SUM(amount_usdt) across BTC option symbols (latest 24h ticker snapshot)
```

## options_trades_24h

Used by: header.

Definition:
```text
options_trades_24h = SUM(trade_count) across BTC option symbols
```

## futures_trades_24h

Used by: header.

Definition:
```text
primary = count from UM BTCUSDT 24h ticker
optional aggregate = UM BTCUSDT + CM BTCUSD_PERP
```

## funding_rate_8h

Used by: header.

Definition:
```text
funding_rate = latest lastFundingRate from UM premium index
funding_interval = fundingIntervalHours from UM fundingInfo
display label "8H" only when funding_interval == 8
```

## atm_strike

Used by: ODTE info card.

Definition:
```text
atm_strike = strike minimizing abs(strike - S)
```

## top_volume_strike

Used by: ODTE info card.

Definition:
```text
top_volume_strike = strike with max premium_by_strike over lookback
```

## top_volume_expiration

Used by: ODTE info card.

Definition:
```text
top_volume_expiration = expiry with max premium_by_expiry_24h
```

## strike_vs_index

Used by: ODTE info card.

Definition:
```text
strike_vs_index = S - reference_strike
reference_strike default: top_volume_strike
```

## big_trade_feed

Used by: right-side event list.

Definition:
```text
big_trade = trade where premium_usdt >= threshold_usdt (default 100000)
sort by event_time desc
```

## option_bubbles_4color

Used by: P1/P2/P3/P4.

Definition:
```text
bubble_size = premium_usdt
bubble_color =
  BUY + CALL -> long_call
  SELL + CALL -> short_call
  BUY + PUT  -> long_put
  SELL + PUT -> short_put
bubble_y = strike
bubble_x = time
```

## bubble_anomaly_score

Used by: alerting + memory evolution features.

Definition:
```text
z1 = zscore(log1p(premium_usdt), rolling_window=24h)
z2 = zscore(trades_per_minute_at_strike, rolling_window=24h)
bubble_anomaly_score = max(z1, z2)
```

Trigger default:
- anomaly when `bubble_anomaly_score >= 3.0` or `premium_usdt >= 100000`.

## 4) Strategy Seed Formulas

## otl_mp_divergence_reversion_buy

Intent:
- when price is far below consensus max pain (or far below dominant expiry max pain), expect reversion upward.

Definition:
```text
mp_consensus = weighted_avg(max_pain across selected frames, weights by premium_24h or oi)
distance = (S - mp_consensus) / mp_consensus
condition_core = distance <= -distance_threshold
condition_context = no confirmed breakdown below major support OR reclaim after reject
score_buy = f(abs(distance), poc_proximity, flow_stabilization, iv_not_extreme)
trigger when condition_core && score_buy >= score_threshold
```

Defaults:
- `distance_threshold = 0.012` (1.2 percent)
- `score_threshold = 0.65`

## otl_mp_divergence_reversion_sell

Intent:
- when price is far above consensus max pain, expect reversion downward.

Definition:
```text
mp_consensus = weighted_avg(max_pain across selected frames, weights by premium_24h or oi)
distance = (S - mp_consensus) / mp_consensus
condition_core = distance >= +distance_threshold
condition_context = no confirmed breakout hold above resistance OR reject and fall back
score_sell = f(abs(distance), poc_proximity, flow_exhaustion, iv_not_extreme)
trigger when condition_core && score_sell >= score_threshold
```

Defaults:
- `distance_threshold = 0.012` (1.2 percent)
- `score_threshold = 0.65`

## 5) Implementation Notes

- For every derived table, store both:
  - computed value
  - `source_ts_min` / `source_ts_max` for traceability.
- Use idempotent upsert keys:
  - strike-level: `[bucket_time, expiry_group, strike, cp]`
  - expiry-level: `[bucket_time, expiry]`
- During websocket disconnects, run REST backfill and fill gaps by timestamp.
- Keep raw events at full granularity; never overwrite raw rows.
