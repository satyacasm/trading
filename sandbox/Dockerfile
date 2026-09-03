# The strategy sandbox image.
#
# Deliberately minimal: a strategy reaches data only through `ctx`, so the
# image needs the analysis libraries the contract's allowlist promises and
# nothing else. Every additional package is additional attack surface and
# additional bytes to pull before a smoke run can start.
#
# Isolation does NOT come from this file. It comes from how the container
# is run -- no network, read-only rootfs, dropped capabilities, resource
# limits, non-root user -- all applied by `trading.agent_contract.sandbox`
# at `docker run` time, so the guarantees live next to the code that
# depends on them rather than in an image someone could rebuild loosely.
FROM python:3.12-slim

# Pinned so a smoke run is reproducible: the same strategy against the same
# image must behave the same way tomorrow.
# pydantic joins numpy and pandas because trading.paper.models is a
# pydantic model set and the real cost model runs in here (design D-S3).
RUN pip install --no-cache-dir --disable-pip-version-check \
      numpy==2.5.2 \
      pandas==2.3.3 \
      pydantic==2.12.3

# Non-root, and with no writable home. The runner writes nothing; the only
# writable surface is the tmpfs the caller mounts at /tmp.
RUN useradd --create-home --shell /usr/sbin/nologin --uid 10001 strategy

# The runner is baked in rather than mounted, so a caller cannot swap it
# for something that skips the guards below. The same is true of the
# runtime: a strategy must not be able to replace the event loop that
# decides what its orders did.
COPY runner.py /opt/runner.py
COPY trading /opt/trading
RUN chmod -R a-w /opt

USER strategy
# The working directory is the ephemeral tmpfs, not a directory of its
# own: source arrives on stdin, so nothing needs a home on disk, and a
# relative path a strategy happens to use lands in scratch space that
# dies with the container.
WORKDIR /tmp
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt

ENTRYPOINT ["python", "/opt/runner.py"]
