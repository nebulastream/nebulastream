/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
class BufferManager;
}

namespace NES::ConnTest
{

/// JSONL <-> native TupleBuffer converter built on the engine's *own* formatter
/// pipeline rather than a hand-rolled JSON codec, so the native tuple layout is
/// authored exactly once (by the engine). It assembles a JSON input-formatter scan
/// feeding a row-layout emit (`decode`) and a row-layout scan feeding a JSON
/// output-formatter emit (`encodeToText`); both directions hold a persistent,
/// already-compiled pipeline stage, so buffers may be fed in succession.
///
/// The sessions hold only the bytes and a `Formatter` — all buffer plumbing,
/// stamping, pool sizing, and text reassembly live here. JSON object keys are the
/// schema's field identifiers, canonicalized (qualifier stripped, lowercased) so
/// they match the conn-test runner's keys whatever the SQL binder folded them to;
/// one `schema` drives both directions, so a native -> JSON -> native round trip is
/// exact.
class Formatter
{
public:
    Formatter(const Schema& schema, std::shared_ptr<BufferManager> bufferManager);
    ~Formatter();

    Formatter(const Formatter&) = delete;
    Formatter& operator=(const Formatter&) = delete;
    Formatter(Formatter&&) = delete;
    Formatter& operator=(Formatter&&) = delete;

    /// Decode a whole JSONL blob into native row-layout buffers. One-shot: the bytes
    /// are fed as a single buffer and the input formatter's end-of-stream tail is
    /// flushed. The emitted buffers are backed by this Formatter's bufferManager, so
    /// the manager (or the Formatter) must outlive them.
    [[nodiscard]] std::vector<TupleBuffer> decode(std::string_view jsonl);

    /// Encode one native row-layout buffer into its JSONL text. The buffer is consumed
    /// as-is: its tuple count and single-chunk identity (sequence / chunk / lastChunk)
    /// are read straight off the buffer — `SourceThread` already stamped them via
    /// `addBufferMetaData` — so the harness sets no metadata here. Reassembles the text
    /// from the formatted parent + child buffers.
    [[nodiscard]] std::string encodeToText(const TupleBuffer& native);

    /// The buffers a JSONL blob decodes into, plus the pool that backs them.
    struct Decoded
    {
        std::shared_ptr<BufferManager> pool; ///< keep alive while `buffers` are used
        std::vector<TupleBuffer> buffers;
    };

    /// Decode `jsonl` into native buffers, sizing a fresh pool so the emit packs
    /// `bufferSize / row_width` tuples per buffer — i.e. ceil(rows / that) buffers
    /// (plus one VARSIZED child per variable-sized field per tuple). This is how the
    /// native sink turns the runner's JSONL into exactly N buffers without the runner
    /// re-deriving the layout. The returned `pool` backs the buffers; hold it (or the
    /// Formatter is gone — it is internal here) for as long as the buffers live.
    [[nodiscard]] static Decoded decodeJsonl(const Schema& schema, std::size_t bufferSize, std::string_view jsonl);

private:
    /// A single compiled pipeline stage plus the collecting context it emits into.
    struct Direction;

    /// Run `direction`'s stage over `buffer` (or stop it when `buffer` is null) and
    /// return the buffers it emitted, releasing the prior call's so a streaming
    /// caller does not pin every buffer it ever produced.
    static std::vector<TupleBuffer> runStage(Direction& direction, const TupleBuffer* buffer);

    Schema schema;
    std::shared_ptr<BufferManager> bufferManager;
    std::unique_ptr<Direction> decodeDir; /// JSONL -> native
    std::unique_ptr<Direction> encodeDir; /// native -> JSONL
};

}
