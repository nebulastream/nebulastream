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
#include <optional>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Map functions read record fields, apply transformations, and can set/update fields.
class MapPhysicalOperator final
{
public:
    MapPhysicalOperator(Record::RecordFieldIdentifier fieldToWriteTo, PhysicalFunction mapFunction);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    [[nodiscard]] MapPhysicalOperator withChild(PhysicalOperator newChild) const;

    void setup(ExecutionContext& ctx, CompilationContext& compCtx) const;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void terminate(ExecutionContext& ctx) const;
    void execute(ExecutionContext& ctx, Record& record) const;

    OperatorId getId() const;
    OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    void setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const;
    void openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void executeChild(ExecutionContext& executionCtx, Record& record) const;
    void terminateChild(ExecutionContext& executionCtx) const;

private:
    Record::RecordFieldIdentifier fieldToWriteTo;
    PhysicalFunction mapFunction;

    std::optional<PhysicalOperator> child;
};

static_assert(PhysicalOperatorConcept<MapPhysicalOperator>);

}
