# Docker Deployment

This folder contains an isolated Docker setup for the `datagen` image, which can run either `datagen` or `datagen_shared`.

## Files

- `Dockerfile`: multi-stage image build for the `datagen` and `datagen_shared` binaries
- `compose.yaml`: local deployment with published TCP ports `8080-8082`
- `config.yaml`: default config for socket output only
- `config.mqtt.yaml`: optional config that also publishes to Mosquitto
- `mosquitto.conf`: minimal broker config used by the optional `mqtt` profile

## Start

From this folder:

```bash
docker compose up --build
```

That builds the image from the repository root, mounts `../data` read-only into `/app/data`, and starts `datagen` with [`config.yaml`](./config.yaml).

To start the shared-stream variant instead:

```bash
DATAGEN_BIN=/usr/local/bin/datagen_shared docker compose up --build
```

This starts the same container service, but runs the `datagen_shared` binary instead of `datagen`.

To verify shared behavior, connect two clients to the same exposed port, for example:

```bash
nc 127.0.0.1 8082
```

```bash
nc 127.0.0.1 8082
```

Both clients should receive identical batches with matching timestamps and ordering.

## MQTT Mode

To start `datagen` together with a local Mosquitto broker:

```bash
DATAGEN_CONFIG=/app/config/config.mqtt.yaml docker compose --profile mqtt up --build
```

To run `datagen_shared` with the MQTT-enabled config:

```bash
DATAGEN_BIN=/usr/local/bin/datagen_shared DATAGEN_CONFIG=/app/config/config.mqtt.yaml docker compose --profile mqtt up --build
```

## Notes

- Edit [`config.yaml`](./config.yaml) or [`config.mqtt.yaml`](./config.mqtt.yaml) to change ports, duration, pattern, or dataset path.
- The mounted dataset path inside the container is `/app/data`.
- Set `DATAGEN_BIN=/usr/local/bin/datagen_shared` to run the shared-stream binary instead of the default `datagen` binary.
- If you want to pass an explicit config path or other arguments to the binary, override the container command or `DATAGEN_CONFIG`.
