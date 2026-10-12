# The Jepsen control node: Leiningen, plus the Docker CLI for the nemesis (it pauses, kills and
# partitions the compose stack's containers through the mounted Docker socket).
FROM clojure:temurin-21-lein-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends docker.io gnuplot-nox git graphviz && rm -rf /var/lib/apt/lists/*
WORKDIR /jepsen
COPY project.clj ./
RUN lein deps
COPY src ./src
RUN lein compile
ENTRYPOINT ["lein", "run"]
