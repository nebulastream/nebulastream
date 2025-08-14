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
#include <ostream>
#include <thread>
#include <utility>

#include <Sources/BlockingSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/AtomicState.hpp>

namespace NES::Sources
{

class BlockingSourceHandle final : public SourceHandle
{
public:
    explicit BlockingSourceHandle(SourceExecutionContext<BlockingSource> ctx);

    BlockingSourceHandle() = delete;
    ~BlockingSourceHandle() override = default;

    BlockingSourceHandle(const BlockingSourceHandle& other) = delete;
    BlockingSourceHandle(BlockingSourceHandle&& other) noexcept = delete;

    BlockingSourceHandle& operator=(const BlockingSourceHandle& other) = delete;
    BlockingSourceHandle& operator=(BlockingSourceHandle&& other) noexcept = delete;

    bool start(EmitFunction&& emitFn) override;
    bool stop() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct Initial
    {
        Initial() = delete;

        SourceExecutionContext<BlockingSource> sourceContext;

        explicit Initial(SourceExecutionContext<BlockingSource>&& context) : sourceContext(std::move(context)) { }
    };

    struct Running
    {
        std::future<void> terminationFuture;
        std::jthread thread;
    };

    struct Stopped
    {
    };

    using BlockingSourceState = AtomicState<Initial, Running, Stopped>;
    BlockingSourceState state;
};

}
