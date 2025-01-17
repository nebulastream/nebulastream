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

#include <memory>

#include <Async/AsyncSourceCoroutineWrapper.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/SourceRunner.hpp>
#include <Util/AtomicState.hpp>

namespace NES::Sources
{

class AsyncSourceRunner : public SourceRunner
{
public:
    explicit AsyncSourceRunner(SourceExecutionContext sourceExecutionContext);
    ~AsyncSourceRunner() override = default;

    AsyncSourceRunner(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner& operator=(const AsyncSourceRunner&) = delete;
    AsyncSourceRunner(AsyncSourceRunner&&) = delete;
    AsyncSourceRunner& operator=(AsyncSourceRunner&&) = delete;

    bool start(SourceReturnType::EmitFunction&& emitFn) override;
    bool stop() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
private:
    struct Initial
    {
        SourceExecutionContext sourceContext;
    };

    struct Running
    {
        std::shared_ptr<AsyncSourceCoroutineWrapper> coroutineWrapper;
    };

    struct Stopped
    {
    };

    using SourceRunnerState = AtomicState<Initial, Running, Stopped>;
    std::unique_ptr<SourceRunnerState> state;
};

}
