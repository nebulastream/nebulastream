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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include "Identifiers/Identifiers.hpp"
#include "Nodes/Node.hpp"
#include "Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp"

namespace NES
{

class LogicalWindowOperator : public WindowOperator
{
public:
    LogicalWindowOperator(const Windowing::LogicalWindowDescriptorPtr& windowDefinition, OperatorId id);
    [[nodiscard]] bool equal(const NodePtr& rhs) const override;
    [[nodiscard]] bool isIdentical(const NodePtr& rhs) const override;
    std::shared_ptr<Operator> copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;

    /**
     * @brief returns the names of every key used in the aggregation
     * @return a vector containing the key names used in the aggregation
     */
    std::vector<std::string> getGroupByKeyNames() const;

protected:
    std::string toString() const override;
};

}
