# Two stages so the runtime image carries the virtualenv but not pip, its cache, or
# the build toolchain. Base image is pinned: "python:3.13" would silently change
# under the deployment.
FROM python:3.13-slim-bookworm AS build
WORKDIR /app
COPY requirements.txt .
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 app
COPY --from=build /venv /venv
WORKDIR /app
COPY ingest.py .
USER 10001
ENV PATH="/venv/bin:$PATH" PYTHONUNBUFFERED=1
EXPOSE 9100
CMD ["python", "ingest.py"]
