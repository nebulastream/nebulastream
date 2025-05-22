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
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>

namespace NES
{


/// Stores the window start and window end field names
struct WindowMetaData
{
    WindowMetaData(std::string windowStartFieldName, std::string windowEndFieldName)
        : windowStartFieldName(std::move(windowStartFieldName)), windowEndFieldName(std::move(windowEndFieldName))
    {
    }

    WindowMetaData() = default;

    std::string windowStartFieldName;
    std::string windowEndFieldName;
};

/**
 * @brief Window operator, which defines the window definition.
 */
class WindowOperator : public LogicalUnaryOperator, public OriginIdAssignmentOperator
{
public:
    WindowOperator(std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, OperatorId id, OriginId originId);
    WindowOperator(std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, OperatorId id);
    /**
    * @brief Gets the window definition of the window operator.
    * @return std::shared_ptr<LogicalWindowDescriptor>
    */
    std::shared_ptr<Windowing::LogicalWindowDescriptor> getWindowDefinition() const;

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    std::vector<OriginId> getOutputOriginIds() const override;

    /**
     * @brief Sets the new origin id also to the window definition
     * @param originId
     */
    void setOriginId(OriginId originId) override;


    WindowMetaData windowMetaData;

protected:
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition;
};

}
