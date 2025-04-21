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
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/UnaryLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{

class WindowOperator : public UnaryLogicalOperator, public OriginIdAssignmentOperator
{
public:
    WindowOperator(std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, OperatorId id, OriginId originId);
    WindowOperator(std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, OperatorId id);
    std::shared_ptr<Windowing::LogicalWindowDescriptor> getWindowDefinition() const;

    std::vector<OriginId> getOutputOriginIds() const override;
    void setOriginId(OriginId originId) override;

    bool isKeyed() const;

    [[nodiscard]] uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    [[nodiscard]] std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> getWindowAggregation() const;
    void setWindowAggregation(const std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>>& windowAggregation);

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;
    void setWindowType(std::shared_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] std::vector<std::shared_ptr<FieldAccessLogicalFunction>> getKeys() const;
    void setOnKey(const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys);

    [[nodiscard]] OriginId getOriginId() const;
    // void setOriginId(OriginId originId);

    ///std::shared_ptr<WindowOperator> clone() const;
    const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

protected:
    std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> windowAggregation;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> onKey;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId = INVALID_ORIGIN_ID;
};

}
