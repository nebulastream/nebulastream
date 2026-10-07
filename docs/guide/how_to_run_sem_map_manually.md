# Running SEM_MAP manually on Rotten Tomatoes reviews

A short recipe for running `SEM_MAP` end to end against a real LLM on one machine: one worker, one
REPL, Ollama on the same host. It classifies 200 movie reviews as `POSITIVE` or `NEGATIVE` and checks
the result against the dataset's own labels.

SEM_MAP is written against the OpenAI-compatible chat completions API: it POSTs to
`<LLM.ENDPOINT>/chat/completions`. Other servers that implement this API, such as vLLM, should work
as well, but **only Ollama has been tested so far** (see
[Other OpenAI-compatible endpoints](#other-openai-compatible-endpoints)).

## Prerequisites

- The dev image `nebulastream/nes-development:local` and a build in `build-docker/`:
  ```shell
  docker run --rm -v "$PWD":"$PWD" -w "$PWD" nebulastream/nes-development:local cmake -B build-docker
  docker run --rm -v "$PWD":"$PWD" -w "$PWD" nebulastream/nes-development:local \
    cmake --build build-docker -j --target nes-single-node-worker nes-repl
  ```
- Ollama listening on `localhost:11434` with the model pulled: `ollama pull llama3.1:latest`.
- `rotten_tomatoes_movie_reviews.csv` (the dataset used by the Python prototype, with the columns
  `reviewText` and `scoreSentiment`).

All commands below run from the repository root. Data and results live in `~/rt`, which is mounted
into the containers as `/data`.

## 1. Prepare the data

The `File` source reads plain CSV without quoting, so the review text is stripped of commas, quotes
and newlines. The ground-truth labels go into a separate file for step 4.

```shell
mkdir -p ~/rt
python3 - /path/to/rotten_tomatoes_movie_reviews.csv <<'EOF'
import csv, os, sys
rows = [r for r in csv.DictReader(open(sys.argv[1])) if r["reviewText"].strip()][:200]
out = os.path.expanduser("~/rt")
with open(f"{out}/rt200.csv", "w") as data, open(f"{out}/rt200_labels.csv", "w") as labels:
    for i, r in enumerate(rows):
        text = r["reviewText"]
        for ch in ',"\n\r':
            text = text.replace(ch, " ")
        data.write(f"{i},{text}\n")
        labels.write(f"{i},{r['scoreSentiment']}\n")
EOF
```

## 2. Write the query

`~/rt/d1q1.sql` (d1q1: the first query on the first dataset, as in the Python reference):

```sql
CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS DATA);
CREATE LOGICAL SOURCE reviews(id UINT64 NOT NULL, reviewText VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR reviews TYPE File SET(
  '/data/rt200.csv' AS "SOURCE".FILE_PATH,
  'localhost:8080'  AS "SOURCE"."HOST",
  'CSV'             AS INPUT_FORMATTER."TYPE");
CREATE SEM_MODEL sentiment
  INPUT (reviewText VARSIZED)
  OUTPUT (sentiment VARSIZED)
  SET ('Determine if the review is positive or negative' AS LLM.PROMPT,
       'http://localhost:11434/v1' AS LLM.ENDPOINT,
       'llama3.1:latest'           AS LLM.MODEL_NAME,
       'POSITIVE,NEGATIVE'         AS LLM.OUTPUT_VALUES);
SELECT id, sentiment FROM SEM_MAP(sentiment, reviews)
  INTO File('/data/rt_out.csv' AS "SINK".FILE_PATH,
            'CSV'              AS "SINK".OUTPUT_FORMAT,
            'localhost:8080'   AS "SINK"."HOST");
```

## 3. Run it

Terminal 1, the worker (stays in the foreground, Ctrl-C stops it). Worker options go after `--`.
`--network host` lets the worker reach Ollama on `localhost:11434`.

```shell
docker run --rm -it --name nes-worker --network host \
  -v "$PWD":"$PWD" -v ~/rt:/data -w "$PWD" \
  nebulastream/nes-development:local \
  build-docker/nes-single-node-worker/nes-single-node-worker -- \
    --grpc=localhost:8080 --data_address=localhost:9090 \
    --worker.query_engine.number_of_worker_threads=4
```

Terminal 2, submit the query. The REPL waits until the file source is exhausted (about 1–2 minutes
for 200 rows at 2–3 rows/s):

```shell
rm -f ~/rt/rt_out.csv
docker run --rm -i --network host \
  -v "$PWD":"$PWD" -v ~/rt:/data -w "$PWD" \
  nebulastream/nes-development:local \
  build-docker/nes-frontend/apps/nes-repl -f JSON --on-exit WAIT_FOR_QUERY_TERMINATION \
  < ~/rt/d1q1.sql
```

Terminal 3, optional, to watch progress:

```shell
watch -n1 'wc -l ~/rt/rt_out.csv; cut -d, -f2 ~/rt/rt_out.csv | sort | uniq -c'
```

Rows arrive in bursts of 20–30 rather than one by one. The sink writes a whole tuple buffer once
every record in it has been answered. That is not batching: every review is still its own LLM call.

## 4. Verify

- **Row count**: `rt_out.csv` has 201 lines, the header plus one line per review. SEM_MAP never drops
  rows.
- **Label split and accuracy against `scoreSentiment`**:
  ```shell
  python3 - <<'EOF'
  import os
  rt = os.path.expanduser("~/rt")
  truth = dict(l.strip().split(",", 1) for l in open(f"{rt}/rt200_labels.csv"))
  got = dict(l.rstrip("\n").split(",", 1) for l in list(open(f"{rt}/rt_out.csv"))[1:])
  empty = sum(1 for v in got.values() if not v)
  correct = sum(1 for k, v in got.items() if v == truth.get(k))
  print(f"rows={len(got)} empty={empty} correct={correct}/{len(got)}")
  EOF
  ```
  Typical for `llama3.1:latest` (8B): roughly 110–120 POSITIVE, 65–75 NEGATIVE, 10–25 empty, and
  74–82 % accuracy.
- **Why a row is empty**: an empty `sentiment` is the default value (`LLM.DEFAULT_VALUE`, empty unless
  set). It is written when the model's answer is not one of the `OUTPUT_VALUES` or the response
  cannot be parsed. The worker logs the reason for every such row:
  ```shell
  grep -o "Semantic answer .* for row\|has no answer for row" singleNodeWorker.log | sort | uniq -c
  ```
  Expect answers such as `neutral`, `mixed` or `-`, mostly on genuinely mixed reviews. SEM_MAP sets
  no temperature, so the set of empty rows changes from run to run. If the endpoint is unreachable,
  the query fails with error 3006 instead of writing defaults.

## SEM_FILTER: keeping only the positive reviews

A model declared **without** an `OUTPUT` clause is a filter model. It adds no column. `SEM_FILTER`
keeps a record only if the model affirms it, which means one of these answers:

- JSON `true`
- the string `"true"` or `"yes"`, in any case
- a non-zero number

Every other answer drops the record, and so do a missing row and an unparseable response.
`OUTPUT_VALUES` and `DEFAULT_VALUE` are rejected on a filter model, because there is no column for
them to describe. Everything else is the same as SEM_MAP: the options, the `ASYNC` execution, and a
transport failure that fails the query with error 3006.

`~/rt/d1q2.sql`, using the same worker and data as above:

```sql
CREATE WORKER 'localhost:8080' SET ('localhost:9090' AS DATA);
CREATE LOGICAL SOURCE reviews(id UINT64 NOT NULL, reviewText VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR reviews TYPE File SET(
  '/data/rt200.csv' AS "SOURCE".FILE_PATH,
  'localhost:8080'  AS "SOURCE"."HOST",
  'CSV'             AS INPUT_FORMATTER."TYPE");
CREATE SEM_MODEL is_positive
  INPUT (reviewText VARSIZED)
  SET ('The review is positive'    AS LLM.PROMPT,
       'http://localhost:11434/v1' AS LLM.ENDPOINT,
       'llama3.1:latest'           AS LLM.MODEL_NAME,
       'ASYNC'                     AS LLM.EXECUTION,
       8                           AS LLM.BATCH_SIZE);
SELECT id FROM SEM_FILTER(is_positive, reviews)
  INTO File('/data/rt_positive.csv' AS "SINK".FILE_PATH,
            'CSV'                   AS "SINK".OUTPUT_FORMAT,
            'localhost:8080'        AS "SINK"."HOST");
```

Run it as in step 3, with `d1q2.sql` as the input. To check the kept ids against the labels:

```shell
python3 - <<'EOF'
import os
rt = os.path.expanduser("~/rt")
truth = dict(l.strip().split(",", 1) for l in open(f"{rt}/rt200_labels.csv"))
kept = {l.strip() for l in list(open(f"{rt}/rt_positive.csv"))[1:]}
positive = {k for k, v in truth.items() if v == "POSITIVE"}
print(f"kept={len(kept)} true_positives={len(kept & positive)} labelled_positive={len(positive)}")
EOF
```

### Fusing SEM_FILTER and SEM_MAP into one prompt

Adjacent semantic operators can share one prompt per batch instead of making one round trip each,
as the Python reference does with `fusion=True`. For this to happen, both models must meet all of
these conditions:

- both set `'TRUE' AS LLM.FUSION`
- both read the same INPUT fields, in the same order
- both use the same endpoint, model and remaining LLM settings, apart from the prompt
- their OUTPUT fields don't collide

```sql
CREATE SEM_MODEL sentiment
  INPUT (reviewText VARSIZED) OUTPUT (sentiment VARSIZED)
  SET ('Determine if the review is positive or negative' AS LLM.PROMPT,
       'http://localhost:11434/v1' AS LLM.ENDPOINT, 'llama3.1:latest' AS LLM.MODEL_NAME,
       'ASYNC' AS LLM.EXECUTION, 8 AS LLM.BATCH_SIZE, 'TRUE' AS LLM.FUSION);
CREATE SEM_MODEL mentions_acting
  INPUT (reviewText VARSIZED)
  SET ('The review mentions the acting' AS LLM.PROMPT,
       'http://localhost:11434/v1' AS LLM.ENDPOINT, 'llama3.1:latest' AS LLM.MODEL_NAME,
       'ASYNC' AS LLM.EXECUTION, 8 AS LLM.BATCH_SIZE, 'TRUE' AS LLM.FUSION);
SELECT id, sentiment FROM SEM_FILTER(mentions_acting, SEM_MAP(sentiment, reviews)) INTO ...;
```

`EXPLAIN (OPTIMIZED) FORMAT TEXT SELECT ...` shows a single
`SEM_FILTER(model: SENTIMENT+MENTIONS_ACTING, ...)` where the pair used to be. Leave `LLM.FUSION`
unset to reproduce the unfused baselines.

## Other OpenAI-compatible endpoints

Untested so far. Any server that implements `POST /v1/chat/completions` should work in principle.
For vLLM, for example, start `vllm serve meta-llama/Llama-3.1-8B-Instruct` (listens on
`localhost:8000`) and change two options in `d1q1.sql`. The model name is the one the server serves,
for vLLM usually the Hugging Face id:

```sql
       'http://localhost:8000/v1'          AS LLM.ENDPOINT,
       'meta-llama/Llama-3.1-8B-Instruct'  AS LLM.MODEL_NAME,
```

If the server requires a key (vLLM started with `--api-key`, or a hosted API), add `'VLLM_API_KEY' AS LLM.API_KEY_ENV`. This option holds
the **name** of an environment variable, never the key itself. The worker reads the variable when it
deploys the query and sends its value as `Authorization: Bearer …`. It must therefore be set in the
worker's container: add `-e VLLM_API_KEY` to the worker's `docker run` in step 3. If the variable is
missing on the worker, the deployment fails.

The step 4 numbers were measured with Ollama and do not carry over to another model or server. If
you try another server, please report whether it works.
