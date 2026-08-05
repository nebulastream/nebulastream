# Ollama image with both the docs-drift-check generation model and the
# embedding model pre-pulled into the image layer, so the CI job never has to
# download either at run time - only pull this image (cached by the container
# registry / runner) and start it. The docs corpus itself is still embedded
# fresh on every run, since jobs land on a random self-hosted worker each
# time and there's no persistent cache to rely on.
FROM ollama/ollama:latest

ARG MODEL=qwen2.5-coder:7b
ARG EMBED_MODEL=mxbai-embed-large
ENV OLLAMA_MODEL=${MODEL}
ENV OLLAMA_EMBED_MODEL=${EMBED_MODEL}

RUN ollama serve & \
    pid=$! && \
    until ollama list >/dev/null 2>&1; do sleep 1; done && \
    ollama pull ${MODEL} && \
    ollama pull ${EMBED_MODEL} && \
    kill $pid

EXPOSE 11434
ENTRYPOINT ["ollama", "serve"]
