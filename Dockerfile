# ベースイメージのバージョンを定義
ARG UV_VERSION=0.6.14
ARG PYTHON_VERSION=3.12

# --- Builder Stage ---
FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-bookworm-slim AS builder

WORKDIR /app

# 依存関係ファイルをコピー
COPY pyproject.toml uv.lock ./

# uvでPython依存関係をインストール (キャッシュマウントは効率的)
# --no-dev は開発依存を除外するため適切
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev --compile-bytecode

COPY vectordb_bench ./vectordb_bench


# --- Runner Stage ---
FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-bookworm-slim AS runner

WORKDIR /app

# builder ステージから仮想環境 (.venv) をコピー
COPY --from=builder /app/.venv .venv

# builder ステージから実行に必要なアプリケーションコードのみをコピー
COPY --from=builder /app/vectordb_bench ./vectordb_bench

# PATH を設定して .venv 内の実行ファイルを使えるようにする
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

EXPOSE 8501
ENTRYPOINT ["uv", "run", "python", "-m", "vectordb_bench"]