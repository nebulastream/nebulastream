# Shared socket writer and handle

## Motivation

`UnixSocketSink` currently calls blocking `send()` while holding its connection lock. If the peer stops reading, an execution worker can remain blocked in that call, and `stop()` cannot acquire the lock to close the connection.

The existing TCP sink avoids this by combining a bounded queue, a dedicated I/O thread, nonblocking writes, cancellable waits, and a bounded shutdown drain. The proposal is to reuse that machinery for Unix-domain sockets rather than implement it again.

This is a design proposal, not an implemented interface. It concerns the sinks; the Unix socket source is outside its scope.

## Proposed shared components

### `SocketHandle`: ownership only

A small, move-only RAII owner of a connected socket descriptor. It closes the descriptor when destroyed and can be transferred from a connector to the writer.

It does **not** manage queues, worker threads, backpressure, formatting, or reconnect policy.

### `SocketChannel`: asynchronous writer

Extract the transport-independent parts of `TCPChannel` into a reusable `SocketChannel`:

- A bounded queue of `TupleBuffer`s and a dedicated I/O thread.
- Partial-write tracking, `EINTR` handling, and nonblocking sends.
- Waiting for socket writability without blocking execution workers.
- Stop-token cancellation, including waking a blocked poll.
- Pending-buffer accounting and draining with a no-progress close timeout.
- Propagating asynchronous I/O errors to producer-side calls.
- Calling an injected connector when a connection is needed.

Each sink owns its **own** channel instance. The component is shared code, not a shared connection or singleton.

The existing API is a starting point: [nes-plugins/Sinks/TCPSink/TCPChannel.hpp:36](nes-plugins/Sinks/TCPSink/TCPChannel.hpp#L36).

## Illustrative interface

```cpp
class SocketChannel
{
public:
    // Returns an owned, connected, nonblocking socket.
    // Returns nullopt on cancellation; throws on connection failure.
    using Connect = std::function<std::optional<SocketHandle>(
        std::chrono::milliseconds timeout,
        std::stop_token stopToken)>;

    SocketChannel(
        Connect connect,
        ChannelOptions options,       // Queue capacity and timeouts.
        std::string connectionHeader = {});

    void start();                     // Launch I/O thread; return immediately.
    SendResult trySend(const TupleBuffer& buffer); // Accepted or Full.
    bool tryClose();                   // Closed, or retry draining later.
    ~SocketChannel();                 // Cancel and join if still running.
};
```

The connector runs on the I/O thread, for both initial connection establishment and any permitted reconnect. It must honor the supplied timeout and stop token; injecting a blocking, uncancellable `connect()` would undermine the design.

The optional connection header is sent by the same asynchronous, cancellable writer. Producing that header remains the sink's responsibility.

## What remains transport-specific

| TCP connector | Unix-domain connector |
| --- | --- |
| Host and port | Socket path |
| DNS resolution and address selection | Path validation and `sockaddr_un` construction |
| TCP connection establishment | Unix-domain connection establishment |
| TCP-specific connection diagnostics | Unix-specific connection diagnostics |

The existing `resolveAddresses()`, `tryConnect()`, and `connectUntil()` logic would belong to the TCP connector rather than the shared writer. Connection establishment currently starts at [nes-plugins/Sinks/TCPSink/TCPChannel.cpp:406](nes-plugins/Sinks/TCPSink/TCPChannel.cpp#L406).

Conceptually, the sinks construct their channels as follows:

```cpp
// TCPSink:
channel(TcpConnector{host, port}, options);

// UnixSocketSink:
channel(UnixConnector{path}, options, formattedSchemaHeader);
```

Plugin registration, configuration schemas, schema formatting, and sink-specific diagnostics remain in their respective plugins. The Unix connector retains `TYPE unix_socket` and the `SOURCE.PATH` / `SINK.PATH` configuration naming.

## Sink integration

The Unix sink must also adopt the TCP sink's nonblocking lifecycle; changing connection establishment alone is not enough:

1. `start()` launches the channel's I/O thread.
2. `execute()` attempts `trySend()`. On a full queue, it uses the existing `BackpressureHandler`, schedules a retry task, and returns to the scheduler.
3. `stop()` attempts `tryClose()`. While draining, it schedules another stop task instead of waiting inside socket I/O.

The reference integration is [nes-plugins/Sinks/TCPSink/TCPSink.cpp:128](nes-plugins/Sinks/TCPSink/TCPSink.cpp#L128).

The I/O thread may wait indefinitely for a non-reading peer during normal operation, but execution workers remain available and upstream progress is regulated through backpressure. Shutdown can cancel that wait. The close timeout measures a lack of completed-buffer progress, not individual bytes written.

## Decisions to make explicitly

- **Reconnect policy:** TCP currently reconnects and replays the entire current buffer, so delivery is best-effort and bytes may be duplicated. Preserve TCP's behavior, but decide whether Unix sockets should reconnect or fail on disconnection rather than changing their semantics accidentally.
- **Headers on reconnect:** Define whether the Unix schema header is sent on every new connection and ensure it uses the cancellable writer path.
- **Configuration:** Decide which queue-capacity and connection/close-timeout options the Unix sink exposes and which defaults it uses.
- **Regression coverage:** Test a peer that stops reading, backpressure and recovery, bounded shutdown, partial writes, and disconnect/reconnect behavior. Existing successful-transfer tests alone do not exercise the reported stall.
