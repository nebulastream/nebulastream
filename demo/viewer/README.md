# TCP Video Viewer

This project shows image frames from one or more TCP servers inside a browser.

## What it does

Each TCP source is expected to stream one CSV record per line with this schema:

```text
id,timestamp,width,height,data
```

Where:
- `id`: frame id
- `timestamp`: frame timestamp
- `width`: image width
- `height`: image height
- `data`: base64 payload

The browser renders each configured source in its own screen card, so you can watch multiple frame streams side by side.

## Important architecture note

A normal webpage cannot open an arbitrary raw TCP socket directly. Because of that, this project contains:

1. a **Node.js bridge** that connects to one or more TCP servers
2. a **web page** that receives frames from the Node bridge over Server-Sent Events (EventSource)

## Supported payload formats

The frontend tries the following automatically:

1. **Encoded image bytes** in base64
   - PNG
   - JPEG
   - GIF
   - WebP
   - BMP
2. **Raw pixel bytes** in base64
   - grayscale (`width * height` bytes)
   - RGB (`width * height * 3` bytes)
   - RGBA (`width * height * 4` bytes)

If your payload uses another custom pixel layout, the decoder can be extended easily.

## Run

No extra package installation is required. Start the app with:

```bash
npm start
```

Open:

```text
http://localhost:8085
```

Use **Add screen** to create as many viewer cards as you need, then enter the TCP host and port for each screen and click **Connect**.

## Run With Docker

Build the image:

```bash
docker build -t tcp-video-viewer .
```

Run the container:

```bash
docker run --rm -p 8085:8085 tcp-video-viewer
```

Then open:

```text
http://localhost:8085
```

If your TCP producer is running on the Docker host, use a host address reachable from inside the container, for example:

- `host.docker.internal` on Docker Desktop
- the host machine IP on Linux
- `--network host` on Linux if that matches your setup and security expectations

## Quick local test

In one terminal, start the viewer:

```bash
node server.js
```

In another terminal, start the included sample TCP producer:

```bash
python3 sample_tcp_source.py
```

Then open `http://localhost:8085`, keep one screen pointed at `127.0.0.1:9000`, and click **Connect**.

If you want to test multiple screens at once, run additional TCP producers on different ports and connect a separate screen card to each source.

## Example TCP line

```text
42,1710785325123,640,480,iVBORw0KGgoAAAANSUhEUgAA...
```

## Assumptions

- each CSV frame is sent as **one line** terminated by `\n`
- the base64 field does **not** contain commas
- width and height are numeric

## Files

- `server.js`: Node bridge and static web server with multi-screen TCP session management
- `public/index.html`: browser UI and frame decoder
- `package.json`: start script
- `Dockerfile`: container image definition
- `sample_tcp_source.py`: optional local test producer

## If your stream is not line-based

If your TCP producer sends records without newline delimiters, the bridge will need a different framing mechanism, such as:
- length-prefixed messages
- fixed-size headers
- sentinel delimiters
