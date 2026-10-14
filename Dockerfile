FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY tender_pipeline ./tender_pipeline

# Never run a crawler as root.
RUN useradd --create-home --uid 10001 pipeline
USER pipeline

ENTRYPOINT ["python", "-m", "tender_pipeline"]
CMD ["run", "--mode", "incremental"]
