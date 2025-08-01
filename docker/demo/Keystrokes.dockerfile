# Keystrokes.dockerfile  – builds the keystrokes image
FROM alpine:3.20
RUN apk add --no-cache bash coreutils netcat-openbsd
WORKDIR /app
# Option A – bake the script into the image
COPY keystrokes.sh /app/keystrokes.sh
RUN chmod +x /app/keystrokes.sh
ENTRYPOINT ["/app/keystrokes.sh"]