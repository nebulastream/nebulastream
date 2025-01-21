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
#include <variant>
#include <utility>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>
#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

struct SourceExecutionContext
{
    SourceExecutionContext() = delete;
    ~SourceExecutionContext() = default;

    SourceExecutionContext(SourceExecutionContext&&) = default;
    SourceExecutionContext& operator=(SourceExecutionContext&&) = default;

    SourceExecutionContext(const SourceExecutionContext&) = delete;
    SourceExecutionContext& operator=(const SourceExecutionContext&) = delete;

    SourceExecutionContext(
        const OriginId originId,
        std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> sourceImpl,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        std::unique_ptr<InputFormatters::InputFormatter> inputFormatter)
        : originId(originId)
        , sourceImpl(std::move(sourceImpl))
        , bufferProvider(bufferProvider)
        , inputFormatter(std::move(inputFormatter))
    {
        PRECONDITION(this->bufferProvider, "BufferProvider must not be null.");
        PRECONDITION(this->inputFormatter, "InputFormatter must not be null.");
    }

    OriginId originId;
    std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> sourceImpl;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter;
};

}
