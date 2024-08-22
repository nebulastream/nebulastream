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

#include <stdint.h>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>

#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>

namespace NES
{
namespace QueryCompilation
{
enum class StreamJoinStrategy : uint8_t;
} /// namespace QueryCompilation
namespace Runtime
{
namespace Execution
{
class ExecutionContext;
class RecordBuffer;
} /// namespace Execution
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution::Operators
{
class NLJProbe : public StreamJoinProbe
{
public:
    /**
     * @brief Constructor for a NLJProbe join phase
     * @param operatorHandlerIndex
     * @param joinSchema
     * @param windowMetaData
     * @param leftEntrySize
     * @param rightEntrySize
     * @param joinStrategy
     * @param windowingStrategy
     * @param withDeletion
     */
    NLJProbe(
        const uint64_t operatorHandlerIndex,
        const JoinSchema& joinSchema,
        Expressions::ExpressionPtr joinExpression,
        const WindowMetaData& windowMetaData,
        const SchemaPtr& leftSchema,
        const SchemaPtr& rightSchema,
        QueryCompilation::StreamJoinStrategy joinStrategy,
        QueryCompilation::WindowingStrategy windowingStrategy,
        bool withDeletion = true);

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

protected:
    const SchemaPtr leftSchema;
    const SchemaPtr rightSchema;
};
}; /// namespace NES::Runtime::Execution::Operators
