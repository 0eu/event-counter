FROM python:3.9-alpine AS builder
WORKDIR /app

# Install dependencies
ADD pyproject.toml poetry.lock /app/
RUN apk add build-base libffi-dev  && \
    python -m pip install --upgrade pip && \
    pip install poetry && \
    poetry config virtualenvs.in-project true && \
    poetry install --no-ansi

FROM python:3.9-alpine as runtime
WORKDIR /app

# Copy dependencies from the builder image
COPY --from=builder /app /app
ADD . /app

# Change user from root to app
RUN adduser app -h /app -u 1000 -g 1000 -DH
USER 1000

ENTRYPOINT ["/app/.venv/bin/python"]
