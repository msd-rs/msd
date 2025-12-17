# Introduction

MSD-RS is a **high-performance time-series database** built on top of [RocksDB](https://rocksdb.org/), written in pure Rust. It is engineered to handle massive amounts of time-series data with speed and efficiency.

## Purpose & Domain

The primary goal of MSD-RS is to provide a robust storage and query engine for high-frequency time-series data.

**Most Suitable Domain:**

- **Quantitative Finance:** Storing and analyzing market data (ticks, candles, quotes).
  - **Zero-Latency OHLCV:** The **Data Pre-aggregation** feature eliminates the need to scan millions of ticks to build K-lines (candles), allowing for instant access to any timeframe (1m, 1d, etc.) as data streams in.
  - **Reactive Data Pipelines:** **Chain Updates** allow new market data to automatically propagate through dependent tables, updating indicators or derived strategies in real-time.
  - **High-Frequency Ready:** Optimized for the high write throughput and low latency reads required by algorithmic trading and market analysis.
