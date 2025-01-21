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
#include <ostream>
#include <thread>

#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <Sources/SourceRunner.hpp>
#include <Util/AtomicState.hpp>

namespace NES::Sources
{

class BlockingSourceRunner : public SourceRunner
{
public:
    explicit BlockingSourceRunner(SourceExecutionContext executionContext);

    BlockingSourceRunner() = delete;
    ~BlockingSourceRunner() override = default;
    BlockingSourceRunner(const BlockingSourceRunner& other) = delete;
    BlockingSourceRunner(BlockingSourceRunner&& other) noexcept = delete;
    BlockingSourceRunner& operator=(const BlockingSourceRunner& other) = delete;
    BlockingSourceRunner& operator=(BlockingSourceRunner&& other) noexcept = delete;

    bool start(EmitFunction&& emitFn) override;
    bool stop() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct Initial
    {
        SourceExecutionContext sourceContext;

        explicit Initial(SourceExecutionContext&& context) : sourceContext(std::move(context)) {}
    };

    struct Running
    {
        std::future<void> terminationFuture;
        std::jthread thread;
    };

    struct Stopped
    {
    };

    using BlockingSourceRunnerState = AtomicState<Initial, Running, Stopped>;
    std::unique_ptr<BlockingSourceRunnerState> state;
};

}
