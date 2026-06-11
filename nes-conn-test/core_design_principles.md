# nes-conn-test — core design principles

The load-bearing design principles of the conn-test framework — each with the
anti-pattern it blocks. Not mechanically enforced; enforced by review and the
battery. If a change fights one of these, it is usually wrong.

### generator-is-truth — the data generator

One `Generator(schema, seed)` is the **single source of all test data**. It adapts
to whatever schema the config under test declares (`parse_source_schema` /
`parse_sink_schema`) — never hardcoding column names, types, counts, or data
characteristics (value size, nullability, the absence of delimiters) — so a new
valid config just works. The rows it generates are *both* the input the connector
is fed and the read-back oracle, so the two can never drift apart, and there are
**no committed fixtures**.

> Anti-pattern: committing test data (`*.csv` / `*.bin` / a golden file);
> hardcoding a schema or specific values (the old CSV wire form assumed no value
> held a `,` or `\n` — exactly the data-characteristics assumption JSONL removed);
> hand-writing an expected result instead of comparing against the generated rows;
> or a loader that generates its own data, canonicalizes for the compare, or
> hand-rolls a wire format instead of calling the framework's codec.

### plugin-isolation

A connector's test surface lives entirely in its plugin dir
(`nes-plugins/<Kind>/<Name>/conn-test/`). The framework stays connector-agnostic;
adding or changing one connector must not require edits to framework code.

> Anti-pattern: special-casing a named connector inside the runner, or reaching
> out of the plugin dir to make one connector work.

### engine-fidelity — exercise NebulaStream as it ships

ConnTest runs the connector in a setup that mirrors a **real deployment**: it uses
NebulaStream's own functionality unmodified and never changes engine code to suit
the test. When the test needs something the engine does not directly expose, the
framework adapts *itself* — composing around the engine (wrap, decorate, drive) —
rather than forking, flag-gating, or special-casing engine internals.

> Anti-pattern: adding a test-only branch, flag, or hook inside engine code to make
> a connector testable; reimplementing engine behavior (a codec, a layout, a parse)
> in the harness or runner instead of calling the engine's. If conntest needs a seam
> the engine lacks, wrap or decorate it from the test side.

### config-fidelity

The framework rewrites **only** the dynamic keys it owns — the endpoint and the
per-test resource name — via the loader's `config_overrides`. Every other value
the author wrote in the `.nesql` passes through verbatim. The
`config_valid`/`config_invalid` scenarios bind the pristine file as-is.

> Anti-pattern: mutating, normalizing, or defaulting an author-set config value;
> "fixing" a config to make a test pass.

### battery-is-ground-truth

Make the code satisfy the battery, never the reverse. **Never weaken, skip,
`xfail`, shrink (N or schema), or special-case a test to turn red green.** New
*behavior* may **add** scenarios; it may not erode existing ones.

> The distinction that matters is intent: a deliberate spec change legitimately
> edits the battery (e.g. the CSV→JSONL switch rewrote the formatted tests). An
> *inconvenient failure* never licenses an edit to the test.

### compose-topology

Service location, port, and credentials come from docker-compose service DNS on
the project network (`compose_service:compose_port`), never from host env vars,
host networking, published ports, or container-IP guessing. This is what makes
the setup portable across architectures/platforms (host vs. CI runner).

> Anti-pattern: `host.docker.internal`, `docker network connect $HOSTNAME`,
> reading a host port from the environment, hardcoding an IP.

### deterministic — replayable data, synchronized control

The whole test is deterministic, in two dimensions, so a failure is reproducible
and the battery never goes flaky.

**Data (replay).** A fixed seed, no wall-clock and no RNG in generation or compare.
A failure replays from its pytest id, which carries N (e.g.
`round_trip_10_buffers-ODBCSource-basic-N10`), and `Dataset.dump()` prints the
oracle rows as JSONL.

**Control (synchronization).** A scenario advances by **handshakes, never by
sleeping**: each control step is one request/reply with the C++ harness, and each
side effect is acked by its component (a SUBACK, a publish-ack, a toxiproxy HTTP
reply, a readiness probe). The source stop is a deterministic quota (see
`externally-bounded`). The layered timeouts — a per-step harness watchdog
(`--step-timeout-ms`), a global framework deadline (`_GLOBAL_DEADLINE_S`), and
bounded loader waits — exist **only as stall-breakers**; a firing timeout is always
a *failure*, never an expected outcome or a synchronization mechanism. Where an
external system has no completion signal, a bounded wait is the backstop, but
correctness is decided by the **content compare, not the clock**.

> Anti-pattern: time- or random-dependent behavior in the data path, or a failure
> that cannot be reproduced from its id; a `time.sleep()` to make a racy step pass,
> a test green only because a wait was "long enough", or treating a timeout firing
> as part of normal flow.

### externally-bounded

A streaming source's read is bounded by the framework's **byte quota** (`FILL`),
not by an end-of-stream. The generated stream carries **data only** — never a
control signal (an EoS marker, sentinel, or poison record). A real MQTT/TCP stream
has no EoS, so the source is tested that way: the framework knows exactly how many
bytes the dataset produces and stops when it has them. EoS is *accepted* where a
connector genuinely signals one (a source that legitimately ends before the quota
returns `eos:true` from `FILL`), but never *required* and never *manufactured*. See
`deterministic` (the quota is the stop) and `generator-is-truth` (the data is
only data).

> Anti-pattern: "fixing" a never-ending source by injecting an EoS/sentinel into
> the generated data and waiting for it in `SourceSession`; making a test pass by
> changing the data or the protocol from the inside instead of bounding it from
> the outside.
