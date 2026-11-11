# Proxy Models: One-Token Answers, Confidence and the SEM_FILTER Cascade

A small model, tuned on a large model's SEM_FILTER answers, comes within about one F1 point of that large model (Emil's
`distill-rotten-tomatoes` study in llm_operator: Qwen3.5-4B with LoRA on 250 labels of Qwen3.5-27B, F1 0.858 against
0.866 on MOVIES_8). This prototype on `proto/sem-proxy` brings that into NES in three steps:

1. **`LLM.ANSWER_MODE 'LABEL'`**: the student's prompt and its one-token answer;
2. **`LLM.CONFIDENCE_FIELD`**: the probability of the answer, from the token's logprobs, as a column;
3. **`LLM.PROXY_MODEL`**: a SEM_FILTER cascade. The student answers the rows it is sure about, and the model itself,
   the teacher, answers the rest plus an audit sample.

```sql
CREATE SEM_MODEL positive
INPUT (reviewText VARSIZED)
SET ('Is this movie review positive?' AS LLM.PROMPT,
     'http://sr650-8:8000/v1' AS LLM.ENDPOINT, 'Qwen/Qwen3.5-27B' AS LLM.MODEL_NAME, 'ASYNC' AS LLM.EXECUTION,
     'student' AS LLM.PROXY_MODEL, 'http://127.0.0.1:8000/v1' AS LLM.PROXY_ENDPOINT,
     '0.9' AS LLM.PROXY_THRESHOLD, '0.02' AS LLM.PROXY_AUDIT_RATE, 'Review' AS LLM.INPUT_LABEL);

SELECT * FROM SEM_FILTER(positive, reviews) INTO out;
```

## 1. LABEL mode

The student of `finetune_label_token.py` is trained on one user message, rendered with thinking off:

```
<instruction> Answer only with true or false.\n\nReview: <text>
```

It answers with one token. Training puts cross-entropy on that token only. A model with `'LABEL' AS LLM.ANSWER_MODE`
sends exactly this, with:
- the model's PROMPT as the instruction;
- `LLM.INPUT_LABEL` as "Review" (default: the INPUT field names);
- the step's values in place of "true or false". A FILTER step has `true`/`false`, a MAP step its OUTPUT_VALUES, listed
  as "a, b or c".

The request carries `max_tokens` 1 (`LLM.MAX_TOKENS` overrides it) and `chat_template_kwargs.enable_thinking` false.
The answer is not a JSON envelope. It is the bare text: surrounding whitespace, quotes and one trailing '.' are removed,
then it is compared with the values, ignoring case. Any other answer is the default: a filter drops the row, a map writes
DEFAULT_VALUE.

Restrictions:
- one step, `BATCH_SIZE` 1 (the student is trained on one row per prompt);
- never fused;
- no IMAGE_FIELDS and no DATASET_PROMPT (the training prompt has none);
- SEM_MAP and SEM_FILTER only.

## 2. Confidence

With `'p_true' AS LLM.CONFIDENCE_FIELD` (LABEL mode only), SEM_CALL requests `logprobs: true, top_logprobs: 20` (vLLM's
default `--max-logprobs`). It reads the alternatives at the first generated token, which is the answer position. It
writes the probability of `true` (FILTER) or of the answered value (MAP), renormalized over the step's values:

- A value's mass is the summed probability of the listed tokens that spell it, ignoring case and surrounding whitespace
  (`true`, `True`, ` true`).
- For a student trained on the lowercase tokens, this equals the training script's softmax over the `[false, true]`
  logits, up to the variants' negligible mass. An untrained model that answers `True` (qwen2.5:0.5b on Ollama) still
  gets a usable number.
- A value not among the 20 alternatives counts as 0. The column is empty when no value is listed at all.

The field is appended to the model's OUTPUT schema at CREATE, so the semantic operators carry it unchanged. SEM_CALL
returns it as the value after the steps' values.

**Servers.** vLLM returns `logprobs` with `top_logprobs` on `/v1/chat/completions`. Ollama 0.30.9 returns them too, but
only for the first generated token (`/v1/chat/completions` and `/api/chat`, streamed or not). That is enough for LABEL
mode, but a probability could not be read inside a longer JSON answer.

## 3. Cascade (SEM_FILTER)

```
PROJECTION(<child fields>)
  SELECTION(__SEM_VERDICT_0 = 'true')
    PROJECTION(*, SEM_CALL(teacher, __SEM_PROXY_ROUTE_0, __SEM_PROXY_VERDICT_0, in...) AS __SEM_VERDICT_0)
      PROJECTION(*, SEM_CALL(student, in...) AS __SEM_PROXY_VERDICT_0, ASYNC_OUTPUT(..,1) AS __SEM_PROXY_ROUTE_0,
                    ASYNC_OUTPUT(..,2) AS __SEM_PROXY_P_0)
```

- **Student call.** The model's payload with PROXY_MODEL, PROXY_ENDPOINT and LABEL mode, and logprobs. It returns its
  verdict, a route and p(true).
- **Route.** A row is `teacher` if max(p, 1 - p) < PROXY_THRESHOLD, or if it has no p. Otherwise it is `audit` if the
  FNV-1a hash of its text falls in the PROXY_AUDIT_RATE share (so the same rows are audited in every run), else
  `student`.
- **Teacher call.** It takes the route and the student's verdict first. A `student` row returns the student's verdict
  with no request. A `teacher` or `audit` row asks the model in its own answer mode (JSON by default), and the teacher's
  verdict counts.
- **Why this shape.** There is no branch and no union. Every row passes both calls, so none is lost or duplicated, and
  ordering is as for any SEM_CALL. Rows that need no teacher pass the second async stage without a request.
- **Statistics.** The latency line adds, for a cascade, the rows decided by the student alone, the unsure rows and the
  audited rows with their disagreements. The audit agreement is the quality signal the system can compute itself.

Restrictions: SEM_FILTER, one step, BATCH_SIZE 1, never fused, no IMAGE_FIELDS, CONFIDENCE_FIELD or DATASET_PROMPT.
PROXY_THRESHOLD is in [0.5, 1], PROXY_AUDIT_RATE in [0, 1].

## Not in this prototype

- A cascade for SEM_MAP (the plan's decision 2), and batching the teacher's rows.
- A label sink for audited rows, and the windows and controller of continuous learning (plan stages 3 and 4).
- Reading p from a JSON answer (it needs per-token logprobs for every token, which vLLM has and Ollama does not).
