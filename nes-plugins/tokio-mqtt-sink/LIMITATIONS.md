# TokioMqttSink — Known Limitations

No known limitations at this time.

Backpressure is handled by the C++ TokioSink wrapper (SinkBackpressureHandler)
which buffers messages and applies pipeline backpressure when the Rust async
channel is full. The Rust sink uses `async_publish()` which yields to Tokio
when the rumqttd router channel is full, naturally throttling consumption from
the async channel.
