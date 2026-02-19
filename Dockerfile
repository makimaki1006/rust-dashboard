# ===== ビルドステージ =====
FROM rust:latest AS builder

# ビルドに必要なシステムライブラリ
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ソースコード + テンプレート（include_str!がコンパイル時に参照）+ Cargo.lock（依存バージョン固定）
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY templates/ templates/
RUN cargo build --release

# ===== ランタイムステージ =====
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# バイナリ
COPY --from=builder /app/target/release/rust_dashboard .

# テンプレート（ランタイムでは不要だがディレクトリ構造維持）
COPY templates/ templates/

# 静的ファイル（CSS/JS）※GeoJSONは除外、gzから起動時解凍
COPY static/css/ static/css/
COPY static/js/ static/js/

# 圧縮データ（起動時に自動解凍）
COPY data/geojson_gz/ data/geojson_gz/
COPY data/job_postings_minimal.db.gz data/job_postings_minimal.db.gz
COPY data/segment_summary.db.gz data/segment_summary.db.gz

EXPOSE 9216

ENV RUST_LOG=info

CMD ["./rust_dashboard"]
