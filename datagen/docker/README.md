# Docker Deployment

This folder contains an isolated Docker setup for the `datagen` service.

## Files

- `Dockerfile`: multi-stage image build for the `datagen` binary
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

## MQTT Mode

To start `datagen` together with a local Mosquitto broker:

```bash
DATAGEN_CONFIG=/app/config/config.mqtt.yaml docker compose --profile mqtt up --build
```

## Notes

- Edit [`config.yaml`](./config.yaml) or [`config.mqtt.yaml`](./config.mqtt.yaml) to change ports, duration, pattern, or dataset path.
- The mounted dataset path inside the container is `/app/data`.
- If you want to pass an explicit config path or other arguments to the binary, override the container command or `DATAGEN_CONFIG`.
