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

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <InputFormatters/InputFormatter.hpp>

namespace NES::Sources
{

class AsyncSourceRunner;
class BlockingSourceRunner;

/// Interface class to handle sources.
/// Created from a source descriptor via the SourceProvider.
/// start(): The underlying source starts consuming data. All queries using the source start processing.
/// stop(): The underlying source stops consuming data, notifying the QueryManager,
/// that decides whether to keep queries, which used the particular source, alive.
class SourceHandle
{
public:
    explicit SourceHandle(
        OriginId originId,
        std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
        std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> sourceImpl,
        std::unique_ptr<InputFormatters::InputFormatter> inputFormatter);

    ~SourceHandle();

    bool start(SourceReturnType::EmitFunction&& emitFn) const;
    bool stop() const;

    friend std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle);

    [[nodiscard]] OriginId getSourceId() const;

private:
    OriginId originId;
    std::variant<std::unique_ptr<AsyncSourceRunner>, std::unique_ptr<BlockingSourceRunner>> sourceRunner;
};

}

/// Specializing the fmt ostream_formatter to accept SourceHandle objects.
/// Allows to call fmt::format("SourceHandle: {}", sourceHandleObject); and therefore also works with our logging.
namespace fmt
{
template <>
struct formatter<NES::Sources::SourceHandle> : ostream_formatter
{
};
}
