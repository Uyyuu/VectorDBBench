FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app
# Update package lists and install necessary packages including libffi-dev, libxml2-dev, and libxslt1-dev
RUN apt-get update && \
    apt-get install -y \
        default-libmysqlclient-dev \
        build-essential \
        pkg-config

# COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy only dependency files first to leverage caching
COPY pyproject.toml uv.lock ./

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Copy the rest of the application code
COPY . .

# Runner Stage
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runner

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
        default-libmysqlclient-dev \
        build-essential \
        pkg-config

# Copy only the necessary files from the builder stage
COPY --from=builder /app/.venv .venv
COPY --from=builder /app .

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8501
# Reset the entrypoint, don't invoke any default command
ENTRYPOINT ["uv", "run", "python", "-m", "vectordb_bench"]
