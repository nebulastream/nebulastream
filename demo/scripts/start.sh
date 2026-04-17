
docker compose up -d --force-recreate --build

bash ./demo/scripts/00_kafka-init-topic.sh

bash ./demo/scripts/02_start_query.sh ./demo/queries/query-engine-wiring.yaml
bash ./demo/scripts/02_start_query.sh ./demo/queries/query-pipe-staple.yaml
bash ./demo/scripts/02_start_query.sh ./demo/queries/query-underbody-screw.yaml

bash ./demo/scripts/01_kafka-consume.sh

docker compose down