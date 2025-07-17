FROM confluentinc/cp-kafka:latest
USER root
RUN apt-get update && apt-get install -y awscli && rm -rf /var/lib/apt/lists/*
USER appuser