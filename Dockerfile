FROM python:3.11-slim

# Slot timing is derived from UTC everywhere; make the container agree.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=UTC \
    NO_BROWSER=1 \
    DASH_HOST=0.0.0.0 \
    MODE=paper_live \
    STATE_ROOT=/data

WORKDIR /app

# Dependencies first so code edits don't invalidate the wheel cache.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fallback for a deploy without a volume attached: the directory must exist or
# the first state write fails. Data written here does NOT survive a redeploy —
# mount a volume at /data to keep the trade archive and the learned parameters.
RUN mkdir -p /data

EXPOSE 8000

# One process: dashboard + supervised bot + the nightly learning cycle.
CMD ["python", "-u", "supervisor.py"]
