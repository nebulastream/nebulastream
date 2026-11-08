# Image Inputs of SEM_MAP and SEM_FILTER

A semantic model can take images: a row names an image file, and `SEM_CALL` sends the image to a vision model together
with the prompt. This is a prototype on `proto/sem-call`; it covers SEM_MAP and SEM_FILTER, the operators that send one
row per request.

```sql
CREATE SEM_MODEL lesion
INPUT (image VARSIZED)
OUTPUT (diagnosis VARSIZED)
SET ('Is the skin lesion in the image benign or malignant? Answer BENIGN or MALIGNANT.' AS LLM.PROMPT,
     'http://localhost:11435/v1' AS LLM.ENDPOINT, 'gemma4:e2b' AS LLM.MODEL_NAME, 'ASYNC' AS LLM.EXECUTION,
     'BENIGN,MALIGNANT' AS LLM.OUTPUT_VALUES,
     'image' AS LLM.IMAGE_FIELDS, '/data/skin' AS LLM.IMAGE_ROOT);

SELECT id, diagnosis FROM SEM_MAP(lesion, scans) INTO out;
```

## Design

1. **Row format: a file path in a VARSIZED field.** An image is not a new data type. The field holds a path, absolute or
   relative to `LLM.IMAGE_ROOT`; without a root, a relative path is resolved against the worker's working directory. The
   unmerged branch `extensible-data-types-poc` has an Image type for raw fixed-size pixel frames. It does not fit
   compressed files of varying size (JPEG, PNG), and it needs a large framework, so the prototype keeps paths. Nothing was
   taken from that branch.
2. **Marking: `LLM.IMAGE_FIELDS`.** The model names the INPUT fields that hold image paths: `'image'`, or several,
   comma-separated (`'front, side'`). The names are spelled as in the INPUT clause. The prompt text stays as it is. The
   payload of the row shows `[image 1]`, `[image 2]`, … in place of each path, in the order the images are attached. The
   path is left out for two reasons: it means nothing to the model, and it may carry the label. The raw skin dataset's
   file names look like `1050_train_malignant_1284.jpg`.
3. **Loading: SEM_CALL reads the file when it builds the request** (`loadImage`, `nes-semantic/src/SemanticImage.cpp`).
   The read is synchronous, on the Async source's event loop. A 20–100 KB image comes from the page cache in tens of
   microseconds, against model calls of hundreds of milliseconds, so it does not block the loop measurably. An asynchronous
   file read would be a framework feature (an async file source for any async function), and it stays a candidate until
   another async function needs it.
   - A row whose image cannot be used gets the step's default value, and a filter drops it, as for any unusable answer.
     No request is sent. Reasons: a missing or unreadable file, a directory, an empty file, an unsupported extension, or
     more than `LLM.IMAGE_MAX_BYTES` (default 8 MB).
   - Each such row logs a warning. The latency line (`NES_SEMANTIC_LATENCY_REPORT=1`) then ends with
     `, <n> rows with an unusable image`; text runs keep their line unchanged.
4. **Request: OpenAI chat format with content parts.** With images, the user message's `content` is a list: the prompt
   text first, then one `{"type": "image_url", "image_url": {"url": "data:<mime>;base64,<bytes>"}}` per image
   (`buildChatRequest`). The MIME type comes from the extension: jpg/jpeg, png, webp or gif. NES does not resize or
   re-encode anything. Without images, the request is byte for byte what it was before: `content` is the prompt string.
5. **Restrictions.** All are checked at `CREATE SEM_MODEL`, except JOIN and AGG, which are checked when a query uses the
   model.
   - `BATCH_SIZE` must be 1: a prompt with several rows would leave the model to work out which image belongs to which
     row.
   - `FUSION` is rejected with images. The design asked for fusion to be off; a silently ignored `'TRUE' AS LLM.FUSION`
     would contradict the catalog's rule of rejecting options that do nothing, so the explicit option is an error, and
     `SemanticFusionRule::canFuse` never fuses an image model.
   - SEM_SCAN and SEM_WINDOW models (`LLM.KIND`) reject `IMAGE_FIELDS` at CREATE. SEM_MATCH (SEM_JOIN) and SEM_AGG
     reject an image model when the query is translated (`InvalidSemanticModel`, error 2048).
   - `IMAGE_ROOT` and `IMAGE_MAX_BYTES` without `IMAGE_FIELDS` are errors.
6. **Mock backend: `imageinfo`.** `SEM_CALL` hands the mock the images it decodes back from the very request the HTTP
   backend would send (`imagesOfChatRequest`). `imageinfo` answers each row with `<mime>:<bytes>:<fnv1a64>` per image, or
   `none`. So the systest compares, end to end, the data URL, the MIME type and the base64 round trip with the committed
   files.

The options travel in the catalog's reflection and in `SEM_CALL`'s payload. The payload carries them only when
`IMAGE_FIELDS` is set, so the payloads and EXPLAIN hashes of text models are unchanged.

## Options

| option | default | meaning |
|---|---|---|
| `LLM.IMAGE_FIELDS` | none | INPUT fields whose values are image paths, comma-separated |
| `LLM.IMAGE_ROOT` | the worker's working directory | the directory relative paths are resolved against |
| `LLM.IMAGE_MAX_BYTES` | 8388608 (8 MB) | larger files are not sent; their rows get the default value |

The path is resolved on the worker that runs the call, so the images must be readable there. On a cluster that means a
shared or replicated directory.

## Tests

- **`nes-semantic/tests/SemanticImageTest.cpp`:**
  - MIME types;
  - base64 against RFC 4648 and over every byte value, plus malformed input;
  - data URLs;
  - loading relative and absolute paths, and every rejection reason;
  - the request with and without images;
  - the mock's `imageinfo`;
  - payload round trip, with no change for text models.
- **`SemanticModelCatalogTest.ImageFieldsAreInputsOfAnUnbatchedMapOrFilterModel`:** validation and reflection.
- **`SemanticMapStatementTest.ImageFieldsNameInputFields`:** binding the options and their error messages.
- **`nes-systests/semantic/SemanticImage.test`:** SEM_MAP and SEM_FILTER end to end against the mock, covering:
  - the data URL round trip;
  - the placeholder in the prompt;
  - restricted answers;
  - missing and oversized files;
  - absolute paths;
  - the rejection by SEM_MATCH and SEM_AGG.

  Its three images, in `testdata/small/semantic/images`, are synthetic (8×8 and 4×4 pixels).

## Future work

- An asynchronous file read in the framework, if another async function needs one.
- Images over HTTP(S) URLs, which the servers fetch themselves. Not done here: the medical images must not leave the
  server.
- Resizing to the model's input size before encoding, to save bandwidth for large photos.
- Images in SEM_SCAN/SEM_WINDOW states, and in JOIN pairs.
