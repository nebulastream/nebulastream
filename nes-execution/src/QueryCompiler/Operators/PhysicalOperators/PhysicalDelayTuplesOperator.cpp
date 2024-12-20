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
#include <sstream>
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDelayTuplesOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalDelayTuplesOperator::PhysicalDelayTuplesOperator(
    OperatorId id, const SchemaPtr& inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay)
    : Operator(id)
    , PhysicalUnaryOperator(id, inputSchema, inputSchema)
    , unorderedness(unorderedness)
    , minDelay(minDelay)
    , maxDelay(maxDelay)
{
}

PhysicalOperatorPtr
PhysicalDelayTuplesOperator::create(SchemaPtr inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay)
{
    return create(getNextOperatorId(), std::move(inputSchema), unorderedness, minDelay, maxDelay);
}
PhysicalOperatorPtr PhysicalDelayTuplesOperator::create(
    OperatorId id, const SchemaPtr& inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay)
{
    return std::make_shared<PhysicalDelayTuplesOperator>(id, inputSchema, unorderedness, minDelay, maxDelay);
}

float PhysicalDelayTuplesOperator::getUnorderedness() const
{
    return unorderedness;
}

uint64_t PhysicalDelayTuplesOperator::getMinDelay() const
{
    return minDelay;
}

uint64_t PhysicalDelayTuplesOperator::getMaxDelay() const
{
    return maxDelay;
}

std::string PhysicalDelayTuplesOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalDelayTuplesOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalDelayTuplesOperator::copy()
{
    auto result = create(id, inputSchema, unorderedness, minDelay, maxDelay);
    result->addAllProperties(properties);
    return result;
}

} // namespace NES::QueryCompilation::PhysicalOperators