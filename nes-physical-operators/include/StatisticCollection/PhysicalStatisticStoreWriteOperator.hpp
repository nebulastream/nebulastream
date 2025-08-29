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

#include <Functions/LogicalFunction.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

class PhysicalStatisticStoreWriteOperator final : public PhysicalOperatorConcept
{
public:
    PhysicalStatisticStoreWriteOperator();
    //static std::shared_ptr<PhysicalOperator>
    //create(OperatorId id, const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema);
    //static std::shared_ptr<PhysicalOperator>
    //create(const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

    const std::string hashFieldName = "hash";
    const std::string typeFieldName = "type";
    const std::string startTsFieldName = "startTs";
    const std::string endTsFieldName = "endTs";
    const std::string dataFieldName = "data";

protected:
    std::optional<PhysicalOperator> child;
};

}
