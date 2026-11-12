## Modelling as Sources

First of all, the implied buffer reuse where we just write into the placeholded fields doesn't happen, because
the Source always allocates a new buffer.
Second of all, it is dangerous to treat the Async Operator output as normal source, because they end up in the Admission
Queue, even though query progress depends on them.
If another source has already filled up the admission queue, the Async Source would not be able to emit, and at some point the sink not be able to ingest.
