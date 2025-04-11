# --- Builder Stage ---
# ベースイメージは slim を使用しており、適切です
FROM ghcr.io/astral-sh/uv:0.6.14-python3.12-bookworm-slim AS builder

WORKDIR /app

# 依存関係ファイルをコピー
COPY pyproject.toml uv.lock ./

# uvでPython依存関係をインストール (キャッシュマウントは効率的)
# --no-dev は開発依存を除外するため適切
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

COPY vectordb_bench ./vectordb_bench


# --- Runner Stage ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runner

WORKDIR /app

# builder ステージから仮想環境 (.venv) をコピー
COPY --from=builder /app/.venv .venv

# builder ステージから実行に必要なアプリケーションコードのみをコピー
COPY --from=builder /app/vectordb_bench ./vectordb_bench
# もしルートに設定ファイルなどが必要な場合は、それらも明示的にコピー
# 例: COPY --from=builder /app/config.yaml ./config.yaml

# PATH を設定して .venv 内の実行ファイルを使えるようにする
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

EXPOSE 8501
# エントリーポイントは変更なし
ENTRYPOINT ["uv", "run", "python", "-m", "vectordb_bench"]