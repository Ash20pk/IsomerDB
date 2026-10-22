# The database image with libfaketime preloaded, for the Jepsen clock-skew nemesis: every process
# reads its clock offset in seconds ("+45", "-30") from /tmp/isomer-faketime, once a second. Monotonic
# clocks are left alone, so timeouts and latches behave. Test-only: never shipped.
#   docker build -f jepsen/skew.Dockerfile -t isomerdb:skew .
FROM isomerdb:dev
RUN apt-get update && apt-get install -y --no-install-recommends libfaketime && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dpkg -L libfaketime | grep 'libfaketime.so.1$' | head -1)" /usr/local/lib/libfaketime.so.1 \
    && echo +0 > /tmp/isomer-faketime && chmod 666 /tmp/isomer-faketime
ENV LD_PRELOAD=/usr/local/lib/libfaketime.so.1 \
    FAKETIME_TIMESTAMP_FILE=/tmp/isomer-faketime \
    FAKETIME_CACHE_DURATION=1 \
    FAKETIME_DONT_FAKE_MONOTONIC=1 \
    FAKETIME_DISABLE_SHM=1
