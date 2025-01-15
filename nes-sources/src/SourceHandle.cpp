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

#include <Sources/SourceHandle.hpp>

#include <memory>
#include <ostream>
#include <type_traits>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>
#include <AsyncSourceRunner.hpp>
#include <BlockingSourceRunner.hpp>
#include "ErrorHandling.hpp"
#include <Sources/SourceReturnType.hpp>

namespace NES::Sources
{

SourceHandle::SourceHandle(
    OriginId originId,
    std::shared_ptr<Memory::AbstractBufferProvider> /*bufferProvider*/,
    std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>> /*sourceImpl*/,
    std::unique_ptr<InputFormatters::InputFormatter> /*inputFormatter*/)
    : originId(originId)
{
}

SourceHandle::~SourceHandle() = default;

bool SourceHandle::start(SourceReturnType::EmitFunction&& emitFn) const
{
    return std::visit(
        [this, emitFn]<typename T>(T&& sourceRunner) -> bool
        {
            if constexpr (std::is_same_v<std::decay_t<T>, AsyncSourceRunner>)
            {
                return sourceRunner->dispatch(AsyncSourceRunner::EventStart{emitFn});
            }
            else if constexpr (std::is_same_v<std::decay_t<T>, BlockingSourceRunner>)
            {
                return sourceRunner->start(emitFn);
            }
            else
            {
                emitFn(originId, SourceReturnType::Error{CannotStartQuery()});
                return false;
            }
        },
        sourceRunner);
}

bool SourceHandle::stop() const
{
    return std::visit(
        []<typename T>(T&& sourceRunner) -> bool
        {
            if constexpr (std::is_same_v<std::decay_t<T>, AsyncSourceRunner>)
            {
                return sourceRunner->dispatch(AsyncSourceRunner::EventStop{});
            }
            else if constexpr (std::is_same_v<std::decay_t<T>, BlockingSourceRunner>)
            {
                return sourceRunner->stop();
            }
            else
            {
                return false;
            }
        },
        sourceRunner);
}

OriginId SourceHandle::getSourceId() const
{
    return originId;
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& /*sourceHandle*/)
{
    return out;
}

}
