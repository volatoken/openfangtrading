# OpenFang Memory Evolution (SQLite + FAISS + LLM Prompt)

Project scaffold cho kiến trúc:

- Market data ingestion -> feature extraction (RSI, MACD)
- Long-term memory bằng `SQLite`
- Vector retrieval qua lớp FAISS-compatible (mặc định dùng cosine search thuần Python)
- Feedback loop WIN/LOSS để tăng/giảm trọng số chiến lược
- Memory summaries + pruning theo chu kỳ
- Semantic ranking + decision Buy/Sell/Hold

## Cấu trúc module

```
openfang_memory_evolution/
├── MarketDataModule
├── FeatureExtractionModule
├── MemoryModule
├── MemoryEvolutionModule
├── SemanticRankingModule
├── LLMModule
├── DecisionMakingModule
├── ExecutionModule
└── app.py
```

## Cách chạy

### One-click trên Windows

Từ thư mục root repo:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1 -RunDemo
```

- Chỉ setup môi trường, không chạy demo:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1
```

- Cài thêm FAISS backend:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup.ps1 -InstallFaiss
```

1. Cài dependency:

```bash
pip install -r requirements.txt
```

Tùy chọn (nếu muốn dùng FAISS backend thật):

```bash
pip install faiss-cpu
```

2. Chạy demo:

```bash
python -m openfang_memory_evolution.app --symbol BTCUSDT --cycles 10
```

## Memory Evolution đang hoạt động thế nào

- Sau mỗi trade:
  - Lưu trade vào SQLite.
  - Nếu có action BUY/SELL và có strategy được chọn: feedback loop cập nhật `wins/losses/weight`.
- Mỗi 5 cycle:
  - Sinh summary cho từng strategy.
  - Prune strategy kém (không đạt `min_win_rate` khi đã đủ `min_trades`).
  - Đồng bộ lại vector index.

## Điểm mở rộng production

- Thay `MarketDataModule.DataCollector` bằng API thật (Binance/Kraken).
- Thay `LLMModule.LLMManager` bằng OpenAI/LLM provider thật.
- Thay `ExecutionModule.APIHandler` bằng client đặt lệnh real.
- Thêm risk management (position sizing, stop loss, max drawdown).
