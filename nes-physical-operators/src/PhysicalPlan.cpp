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
#include <PhysicalPlan.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{
namespace
{
void appendOptionalSchemaSignature(std::ostringstream& signature, const std::optional<Schema>& schema)
{
    if (schema.has_value())
    {
        signature << schema.value();
    }
    else
    {
        signature << "<none>";
    }
}

void appendOptionalLayoutSignature(std::ostringstream& signature, const std::optional<MemoryLayoutType>& layout)
{
    if (layout.has_value())
    {
        signature << static_cast<int>(layout.value());
    }
    else
    {
        signature << "<none>";
    }
}

void appendWrapperSignature(
    const std::shared_ptr<PhysicalOperatorWrapper>& wrapper,
    std::ostringstream& signature,
    std::unordered_map<const PhysicalOperatorWrapper*, uint64_t>& wrapperOrdinals,
    uint64_t& nextWrapperOrdinal)
{
    const auto wrapperPtr = wrapper.get();
    if (const auto it = wrapperOrdinals.find(wrapperPtr); it != wrapperOrdinals.end())
    {
        signature << "{ref=w" << it->second << "}";
        return;
    }

    const auto wrapperOrdinal = nextWrapperOrdinal++;
    wrapperOrdinals.emplace(wrapperPtr, wrapperOrdinal);

    signature << "{w=" << wrapperOrdinal;
    signature << ",op=" << wrapper->getPhysicalOperator().getSignature();
    signature << ",inSchema=";
    appendOptionalSchemaSignature(signature, wrapper->getInputSchema());
    signature << ",outSchema=";
    appendOptionalSchemaSignature(signature, wrapper->getOutputSchema());
    signature << ",inLayout=";
    appendOptionalLayoutSignature(signature, wrapper->getInputMemoryLayoutType());
    signature << ",outLayout=";
    appendOptionalLayoutSignature(signature, wrapper->getOutputMemoryLayoutType());
    signature << ",loc=" << static_cast<int>(wrapper->getPipelineLocation());
    signature << ",children=[";
    for (const auto& child : wrapper->getChildren())
    {
        appendWrapperSignature(child, signature, wrapperOrdinals, nextWrapperOrdinal);
    }
    signature << "]}";
}
}

PhysicalPlan::PhysicalPlan(
    QueryId id,
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators,
    ExecutionMode executionMode,
    uint64_t operatorBufferSize,
    std::string originalSql)
    : queryId(id)
    , rootOperators(std::move(rootOperators))
    , executionMode(executionMode)
    , operatorBufferSize(operatorBufferSize)
    , originalSql(std::move(originalSql))
{
    for (const auto& rootOperator : this->rootOperators)
    {
        PRECONDITION(rootOperator->getPhysicalOperator().tryGet<SourcePhysicalOperator>(), "Expects SourcePhysicalOperator as roots");
    }
}

std::string PhysicalPlan::toString() const
{
    std::stringstream stringstream;
    auto dumpHandler = QueryConsoleDumpHandler<PhysicalPlan, PhysicalOperatorWrapper>(stringstream, true);
    for (const auto& rootOperator : rootOperators)
    {
        dumpHandler.dump(*rootOperator);
    }
    return stringstream.str();
}

QueryId PhysicalPlan::getQueryId() const
{
    return queryId;
}

const PhysicalPlan::Roots& PhysicalPlan::getRootOperators() const
{
    return rootOperators;
}

ExecutionMode PhysicalPlan::getExecutionMode() const
{
    return executionMode;
}

uint64_t PhysicalPlan::getOperatorBufferSize() const
{
    return operatorBufferSize;
}

const std::string& PhysicalPlan::getOriginalSql() const
{
    return originalSql;
}

std::string PhysicalPlan::getSignature() const
{
    std::ostringstream signature;
    signature << "physical-plan-canonical";
    signature << "|mode=" << static_cast<int>(executionMode);
    signature << "|buffer=" << operatorBufferSize;
    signature << "|rootCount=" << rootOperators.size();
    signature << "|roots=[";
    std::unordered_map<const PhysicalOperatorWrapper*, uint64_t> wrapperOrdinals;
    uint64_t nextWrapperOrdinal = 0;
    for (const auto& rootWrapper : rootOperators)
    {
        appendWrapperSignature(rootWrapper, signature, wrapperOrdinals, nextWrapperOrdinal);
    }
    signature << "]";
    return signature.str();
}

std::ostream& operator<<(std::ostream& os, const PhysicalPlan& plan)
{
    os << plan.toString();
    return os;
}
}
