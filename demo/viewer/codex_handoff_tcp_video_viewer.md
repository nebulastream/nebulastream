# Codex Handoff: TCP Video Viewer

## Goal
Continue work on a small project that shows image frames from a TCP source in a browser, like a video stream.

## Important architecture constraint
A normal browser page cannot open an arbitrary raw TCP socket directly.
So the project uses:

1. a Node.js backend bridge that opens the raw TCP connection to the TCP server
2. Server-Sent Events (SSE) from the backend to the browser
3. a browser page that decodes base64 frame payloads and renders them to a canvas

## Input format
The TCP source sends CSV rows with this schema:

`id,timestamp,width,height,data`

Where:
- `id`: frame identifier
- `timestamp`: frame timestamp
- `width`: image width
- `height`: image height
- `data`: base64 payload

## Decoding behavior
The frontend should support:
- base64-encoded image file bytes such as PNG, JPEG, GIF, WebP, BMP
- raw base64 pixel buffers inferred from `width` and `height`:
  - grayscale
  - RGB
  - RGBA

## Current project state
The project was previously created as:
- `server.js` — Node TCP bridge + HTTP server
- `public/index.html` — browser UI
- `sample_tcp_source.py` — optional local test TCP producer
- `README.md`

Then the UI was edited manually and some sections were deleted.
That broke the page because the JavaScript still referenced DOM elements that no longer existed.

## What was fixed most recently
The user asked to simplify the JavaScript instead of restoring all removed UI sections.

A simplified `index.html` was produced that keeps only:
- TCP host input
- TCP port input
- target playback FPS input
- Connect button
- Disconnect button
- connection status
- canvas rendering
- last frame id
- last timestamp
- event log

The simplified page intentionally removes JS dependencies on deleted UI elements such as:
- framesReceived
- framesDisplayed
- queueSize
- lastResolution
- lastMode
- lastLatency
- lastError

## Frontend behavior that should remain
The simplified page should still:
- open an `EventSource('/events')`
- listen for:
  - `status`
  - `warning`
  - `frame`
- POST to:
  - `/api/connect`
  - `/api/disconnect`
- GET:
  - `/api/status`
- decode incoming base64 data
- render frames to a `<canvas>`
- pace rendering according to target FPS
- update:
  - connection status
  - last frame id
  - last timestamp
  - event log

## Core rendering logic
Keep these concepts:
- queue incoming frames
- decode on demand
- render at target FPS with `requestAnimationFrame`
- drop old frames if queue grows too large, to preserve near-real-time playback
- fit the source image into the canvas while preserving aspect ratio

## Likely functions in the frontend
Useful functions or equivalents:
- `log(message)`
- `updateStatus(connected, extra)`
- `ensureEventStream()`
- `postJson(url, body)`
- `base64ToBytes(base64Value)`
- `detectMime(bytes)`
- `rawPixelsToImageData(bytes, width, height)`
- `decodeFrame(frame)`
- `drawFit(sourceWidth, sourceHeight, drawSource)`
- `renderNextFrame()`
- `renderLoop(now)`

## What the user wants now
The user wants a Codex-compatible context export from this chat so work can continue on their PC.

## Good next step for Codex
Ask Codex to:
1. take the current simplified `index.html`
2. verify that all referenced DOM elements actually exist
3. keep only the minimal UI still present
4. preserve SSE frame streaming and canvas playback
5. avoid any dead code from previously removed stats/detail panels

## Suggested Codex task prompt
Use this with Codex:

---
You are continuing a small TCP video viewer project.

Architecture:
- Browser cannot connect to raw TCP directly.
- Node backend connects to the TCP server.
- Browser receives frames through Server-Sent Events.
- Frontend displays frames on a canvas like a video stream.

Input rows from TCP:
id,timestamp,width,height,data

Where `data` is base64.

Requirements:
- Keep the frontend minimal.
- Only retain UI for host, port, playback FPS, connect/disconnect, status, canvas, last frame id, last timestamp, and log.
- Remove JS references to any deleted DOM elements.
- Keep support for base64 image bytes and raw grayscale/RGB/RGBA payloads.
- Keep queueing, target-FPS playback, and aspect-ratio-preserving rendering.
- Keep `/events`, `/api/connect`, `/api/disconnect`, and `/api/status` integration intact.

Please inspect the current `index.html`, simplify it cleanly, and return the corrected file.
---
