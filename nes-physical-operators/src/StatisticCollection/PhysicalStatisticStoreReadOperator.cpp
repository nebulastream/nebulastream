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

#include <StatisticCollection/PhysicalStatisticStoreReadOperator.hpp>

namespace NES
{

PhysicalStatisticStoreReadOperator::PhysicalStatisticStoreReadOperator()
{
}

// std::shared_ptr<PhysicalOperator> PhysicalStatisticStoreReadOperator::create(
//     OperatorId id, const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema)
// {
//     return std::make_shared<PhysicalStatisticStoreReadOperator>(id, inputSchema, outputSchema);
// }
//
// std::shared_ptr<PhysicalOperator>
// PhysicalStatisticStoreReadOperator::create(const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema)
// {
//     return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema));
// }

std::optional<PhysicalOperator> PhysicalStatisticStoreReadOperator::getChild() const
{
    return child;
}

void PhysicalStatisticStoreReadOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
