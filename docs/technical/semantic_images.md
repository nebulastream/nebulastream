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

| `LLM.IMAGE_ENCODING` | `PATH` | `PATH`: the field holds a file path; `BASE64`: the field holds the image bytes as base64 |

The path is resolved on the worker that runs the call, so the images must be readable there. On a cluster that means a
shared or replicated directory, or `BASE64`.

## Images in the stream: `LLM.IMAGE_ENCODING 'BASE64'`

With `'BASE64' AS LLM.IMAGE_ENCODING` the image field carries the image itself, so the worker needs no access to the
files. The source writes each image as padded standard base64 (RFC 4648 section 4: `A–Z a–z 0–9 + /`, `=` padding, no
line breaks, no `data:` prefix) into the VARSIZED field, e.g. one CSV line `id,<base64>` per image; base64 has no comma,
quote or line break, so it needs no CSV quoting.

```sql
CREATE SEM_MODEL pets
INPUT (image VARSIZED)
OUTPUT (animal VARSIZED)
SET ('Which animal is shown: cat or dog?' AS LLM.PROMPT, 'cat,dog' AS LLM.OUTPUT_VALUES, ...,
     'image' AS LLM.IMAGE_FIELDS, 'BASE64' AS LLM.IMAGE_ENCODING);
```

- **No decoding, no re-encoding** (`imageFromBase64`): `SEM_CALL` checks that the text is padded standard base64
  (one pass over the characters), computes the decoded size from the length for `LLM.IMAGE_MAX_BYTES`, and decodes only
  the first 16 characters to read the file signature, which gives the MIME type: JPEG `FF D8 FF`, PNG `89 50 4E 47 0D 0A
  1A 0A`, WebP `RIFF....WEBP`, GIF `GIF87a`/`GIF89a`. The text then goes into `data:<mime>;base64,<text>` as it is.
- **The same request as `PATH`:** for a file with the right extension, the data URL is byte for byte the one `PATH`
  builds from the file, because both are standard base64 of the same bytes (`Base64FieldGivesTheSameRequestAsTheFile`).
- **Unusable values** get the default, as in `PATH` mode: empty, not base64 (a path, URL-safe base64, a line break,
  a `data:` URL), larger than `IMAGE_MAX_BYTES` once decoded, or not one of the four image signatures. The warning never
  repeats the field.
- `IMAGE_ROOT` is rejected with `BASE64`: no file is read, and the catalog rejects options that do nothing.
  `IMAGE_ENCODING` without `IMAGE_FIELDS` is rejected too. The payload and the catalog carry the option only for
  `BASE64`, so `PATH` models keep their payload and EXPLAIN hash.
- **Cost:** the tuple carries about 4/3 of the image size in the VARSIZED field (a 47 KB JPEG is a 63 KB field) instead
  of a path of a few dozen bytes. Measured on 2,000 pet photos in `sem-proxy-REPORT.md` (12 Nov).

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
- **`SemanticImageTest` for `BASE64`:** encoding names, the decoded size without decoding, the MIME signatures, the same
  request as from the file, and every rejection.
- **`nes-systests/semantic/SemanticImageBase64.test`:** the same three images as base64 in a CSV file
  (`testdata/small/semantic/images_base64.csv`) give the same mock answers as from disk; a 100,000-byte image in a
  133,336-character field; the placeholder in the prompt; `IMAGE_MAX_BYTES`; a path and a non-image as unusable values;
  SEM_FILTER.
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
