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

    static std::optional<PhysicalOperator> getChild();
    static SinkPhysicalOperator withChild(PhysicalOperator);

    static void setup(ExecutionContext& ctx, CompilationContext& compCtx);
    static void open(ExecutionContext& ctx, RecordBuffer& recordBuffer);
    static void close(ExecutionContext& ctx, RecordBuffer& recordBuffer);
    static void terminate(ExecutionContext& ctx);
    static void execute(ExecutionContext& ctx, Record& record);

    [[nodiscard]] SinkDescriptor getDescriptor() const;

    bool operator==(const SinkPhysicalOperator& other) const;

    [[nodiscard]] OperatorId getId() const;
    OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    static void setupChild(ExecutionContext&, CompilationContext&);
    static void openChild(ExecutionContext&, RecordBuffer&);
    static void closeChild(ExecutionContext&, RecordBuffer&);
    static void executeChild(ExecutionContext&, Record&);
    static void terminateChild(ExecutionContext&);

private:
    SinkDescriptor descriptor;
};

static_assert(PhysicalOperatorConcept<SinkPhysicalOperator>);

}
