# Ollama image with both the docs-drift-check generation model and the
# embedding model pre-pulled into the image layer, so the CI job never has to
# download either at run time - only pull this image (cached by the container
# registry / runner) and start it. The docs corpus itself is still embedded
# fresh on every run, since jobs land on a random self-hosted worker each
# time and there's no persistent cache to rely on.
#
# This image is also used as the docs-diff-check.yml job's own container
# (not just a services: sidecar) - ollama serve is the ENTRYPOINT, which
# doubles as the long-running process that keeps a GitHub Actions job
# container alive while steps are exec'd into it. gh/git/python3 are here
# so the job's steps (checkout, gh pr list/diff, the check script) can run
# in the same container instead of depending on tooling the self-hosted
# runner host happens to have.
FROM ollama/ollama:latest

ARG MODEL=qwen2.5-coder:7b
ARG EMBED_MODEL=mxbai-embed-large
ENV OLLAMA_MODEL=${MODEL}
ENV OLLAMA_EMBED_MODEL=${EMBED_MODEL}
# Ollama stores/looks up models under $HOME/.ollama/models by default. HOME
# is /root at build time (where the models below get pulled to), but GitHub
# Actions job containers force HOME=/github/home at runtime - without this,
# ollama looks in the wrong, empty directory and reports the model missing
# even though it's sitting right here in the image.
ENV OLLAMA_MODELS=/root/.ollama/models

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg git python3 && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends gh && \
    rm -rf /var/lib/apt/lists/*

RUN ollama serve & \
    pid=$! && \
    for i in $(seq 1 60); do \
        ollama list >/dev/null 2>&1 && break; \
        kill -0 $pid 2>/dev/null || { echo "ollama serve died on startup" >&2; exit 1; }; \
        sleep 1; \
    done && \
    ollama list >/dev/null 2>&1 || { echo "ollama serve did not become ready in time" >&2; exit 1; } && \
    ollama pull ${MODEL} && \
    ollama pull ${EMBED_MODEL} && \
    kill $pid

EXPOSE 11434
ENTRYPOINT ["ollama", "serve"]
