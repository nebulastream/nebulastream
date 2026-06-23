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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// InMemoryFileSource loads the ENTIRE backing file into RAM in its constructor and then produces
/// raw buffers by memcpy-ing slices of that in-memory blob in fillTupleBuffer().
///
/// Why a separate source: through a real File source the producer is read-bound -- one io-thread's
/// pread/fill/enqueue caps end-to-end at ~3.5 GB/s regardless of worker count (see
/// producer-bottleneck-findings.md). To measure how fast a single producer can feed the formatter
/// when it is NOT read-bound, we take the read out of the measured window. The constructor runs at
/// query-registration time (SourceProvider::lower), which is BEFORE systest -b stamps the `running`
/// timestamp it measures from (the `open()` hook would NOT work -- it runs inside the producer
/// thread that start() spawns asynchronously and races the timer). So the file load is excluded
/// from the benchmarked time; only the RAM->buffer memcpy production is measured.
///
/// Being a BlockingSource, it also gets one dedicated producer thread and avoids the async runner's
/// strictly-sequential one-buffer-at-a-time await.
///
/// PREFILL mode (env `NES_INMEM_PREFILL=1`, buffer size `NES_INMEM_BUFSIZE`, default 128 KiB): instead of
/// holding a byte blob and memcpy-ing per buffer, the ctor reads the file directly into a private pool of
/// ready TupleBuffers and the runner drains them via takePreFilledBuffer() -- ZERO memcpy in the timed
/// window. This isolates whether the per-buffer producer memcpy is what caps the single producer at
/// ~8.5 GB/s and what costs the lone worker the 3.94->4.7 gap (see producer-bottleneck-findings.md).
/// PARALLEL threading mode only.
class InMemoryFileSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "InMemory";

    explicit InMemoryFileSource(const SourceDescriptor& sourceDescriptor);
    ~InMemoryFileSource() override = default;

    InMemoryFileSource(const InMemoryFileSource&) = delete;
    InMemoryFileSource& operator=(const InMemoryFileSource&) = delete;
    InMemoryFileSource(InMemoryFileSource&&) = delete;
    InMemoryFileSource& operator=(InMemoryFileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// No-op: the file is already resident (loaded in the ctor).
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Prefill mode: hand the runner the next pre-filled buffer (nullopt = end-of-stream).
    [[nodiscard]] bool preFillsBuffers() const override { return prefillMode; }
    [[nodiscard]] std::optional<TupleBuffer> takePreFilledBuffer() override;

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;

    /// memcpy mode (default): the whole file as a byte blob, sliced per fillTupleBuffer().
    std::string blob; ///< loaded once in the ctor (outside the timed window)
    size_t cursor = 0; ///< next unread byte offset into `blob`

    /// prefill mode (NES_INMEM_PREFILL=1): the file pre-read into ready buffers owned by a private pool.
    bool prefillMode = false;
    std::shared_ptr<BufferManager> prefillPool; ///< owns the pre-filled buffers; outlives them
    std::vector<TupleBuffer> prefilled; ///< pre-filled buffers, handed out in order
    size_t prefillCursor = 0; ///< next index into `prefilled`
};

struct ConfigParametersInMemory
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string("file_path"),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
