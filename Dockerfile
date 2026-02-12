FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY michel-bot /usr/local/bin/michel-bot
EXPOSE 8080
ENTRYPOINT ["michel-bot"]
