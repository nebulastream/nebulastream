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
#include <Nautilus/Interface/Record.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
class SinkPhysicalOperator final
{
public:
    explicit SinkPhysicalOperator(const SinkDescriptor& descriptor);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    SinkPhysicalOperator withChild(PhysicalOperator) const;

    void setup(ExecutionContext& ctx, CompilationContext& compCtx) const;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const;
    void terminate(ExecutionContext& ctx) const;
    void execute(ExecutionContext& ctx, Record& record) const;

    [[nodiscard]] SinkDescriptor getDescriptor() const;

    bool operator==(const SinkPhysicalOperator& other) const;

    OperatorId getId() const;
    OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    void setupChild(ExecutionContext&, CompilationContext&) const;
    void openChild(ExecutionContext&, RecordBuffer&) const;
    void closeChild(ExecutionContext&, RecordBuffer&) const;
    void executeChild(ExecutionContext&, Record&) const;
    void terminateChild(ExecutionContext&) const;

private:
    SinkDescriptor descriptor;
};

static_assert(PhysicalOperatorConcept<SinkPhysicalOperator>);

}
