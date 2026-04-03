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

#include <future>
#include <memory>
#include <optional>
#include <ostream>
#include <thread>
#include <utility>

#include <Blocking/BlockingSourceRunner.hpp>
#include <Decoders/Decoder.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/AtomicState.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

class BlockingSourceHandle final : public SourceHandle
{
public:
    /// Obtains decoder implementation if the source produces encoded data.
    explicit BlockingSourceHandle(
        BackpressureListener backpressureListener,
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        SourceRuntimeConfiguration configuration,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        std::unique_ptr<BlockingSource> sourceImplementation,
        std::optional<std::unique_ptr<Decoder>> decoder);

    BlockingSourceHandle() = delete;
    ~BlockingSourceHandle() override = default;

    BlockingSourceHandle(const BlockingSourceHandle& other) = delete;
    BlockingSourceHandle(BlockingSourceHandle&& other) noexcept = delete;

    BlockingSourceHandle& operator=(const BlockingSourceHandle& other) = delete;
    BlockingSourceHandle& operator=(BlockingSourceHandle&& other) noexcept = delete;

    bool start(SourceReturnType::EmitFunction&& emitFunction) override;
    void stop() override;
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Used to print the data source via the overloaded '<<' operator.
    std::unique_ptr<BlockingSourceRunner> sourceThread;
    // struct Initial
    // {
    //     Initial() = delete;
    //
    //     SourceExecutionContext<BlockingSource> sourceContext;
    //
    //     explicit Initial(SourceExecutionContext<BlockingSource>&& context) : sourceContext(std::move(context)) { }
    // };
    //
    // struct Running
    // {
    //     std::future<void> terminationFuture;
    //     std::jthread thread;
    // };
    //
    // struct Stopped
    // {
    // };
    //
    // using BlockingSourceState = AtomicState<Initial, Running, Stopped>;
    // BlockingSourceState state;
};

}
