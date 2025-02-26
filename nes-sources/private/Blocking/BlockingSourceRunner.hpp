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

#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <stop_token>
#include <utility>
#include <variant>

#include <Sources/BlockingSource.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceUtility.hpp>
#include <ErrorHandling.hpp>
#include "Identifiers/Identifiers.hpp"

namespace NES::Sources
{

/// RAII-Wrapper around source open and close
struct BlockingSourceWrapper
{
    explicit BlockingSourceWrapper(BlockingSource& source) : source(source) { source.open(); }
    BlockingSourceWrapper(const BlockingSourceWrapper& other) = delete;
    BlockingSourceWrapper(BlockingSourceWrapper&& other) noexcept = delete;
    BlockingSourceWrapper& operator=(const BlockingSourceWrapper& other) = delete;
    BlockingSourceWrapper& operator=(BlockingSourceWrapper&& other) noexcept = delete;

    ~BlockingSourceWrapper()
    {
        /// Throwing in a destructor would terminate the application
        try
        {
            source.close();
        }
        catch (...)
        {
            tryLogCurrentException();
        }
    }
    BlockingSource& source; ///NOLINT Source handle should never outlive the source
};

class BlockingSourceRunner
{
public:
    using EmitFn = std::function<void(IOBuffer)>;

    BlockingSourceRunner(
        std::promise<void> terminationPromise,
        EmitFunction&& emitFn,
        std::unique_ptr<SourceExecutionContext<BlockingSource>> executionContext);

    BlockingSourceRunner() = delete;
    ~BlockingSourceRunner() = default;

    BlockingSourceRunner(BlockingSourceRunner&&) = default;
    BlockingSourceRunner& operator=(BlockingSourceRunner&&) = default;

    BlockingSourceRunner(const BlockingSourceRunner&) = delete;
    BlockingSourceRunner& operator=(const BlockingSourceRunner&) = delete;

    void runningRoutine(const std::stop_token& stopToken) const;

    void operator()(const std::stop_token& stopToken);

private:
    std::promise<void> terminationPromise;
    std::unique_ptr<SourceExecutionContext<BlockingSource>> executionContext;
    EmitFunction emitFn;
};

}