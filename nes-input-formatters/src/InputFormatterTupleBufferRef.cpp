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

#include <InputFormatters/InputFormatterTupleBufferRef.hpp>

#include <memory>
#include <ostream>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>

namespace NES
{

void InputFormatterTupleBufferRef::readBuffer(
    ExecutionContext& executionCtx, const RecordBuffer& recordBuffer, const ExecuteChildFn& executeChild) const
{
    this->inputFormatter->readBuffer(executionCtx, recordBuffer, executeChild);
}

nautilus::val<bool> InputFormatterTupleBufferRef::indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arenaRef) const
{
    return this->inputFormatter->indexBuffer(recordBuffer, arenaRef);
}

std::ostream& operator<<(std::ostream& os, const InputFormatterTupleBufferRef& inputFormatterTupleBufferRef)
{
    return inputFormatterTupleBufferRef.inputFormatter->toString(os);
}

}
