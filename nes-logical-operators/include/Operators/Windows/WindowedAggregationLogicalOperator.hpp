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
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{

class WindowedAggregationLogicalOperator : public WindowOperator
{
public:
    static constexpr std::string_view NAME = "WindowedAggregation";

    WindowedAggregationLogicalOperator(const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& onKey,
                          std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> windowAggregation,
                          std::shared_ptr<Windowing::WindowType> windowType,
                          OperatorId id);
    [[nodiscard]] bool operator==(Operator const& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    std::shared_ptr<Operator> clone() const override;
    bool inferSchema() override;

    std::vector<std::string> getGroupByKeyNames() const;

    bool isKeyed() const;

    [[nodiscard]] uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    [[nodiscard]] std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> getWindowAggregation() const;
    void setWindowAggregation(const std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>>& windowAggregation);

    [[nodiscard]] std::shared_ptr<Windowing::WindowType> getWindowType() const;

    void setWindowType(std::shared_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] std::vector<std::shared_ptr<FieldAccessLogicalFunction>> getKeys() const;
    void setOnKey(const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& onKeys);

    [[nodiscard]] uint64_t getAllowedLateness() const;
    [[nodiscard]] OriginId getOriginId() const;
    const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

    void setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft);
    void setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight);

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;

    [[nodiscard]] SerializableOperator serialize() const override;

protected:
    std::string toString() const override;

private:
    std::vector<std::shared_ptr<Windowing::WindowAggregationFunction>> windowAggregation;
    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> onKey;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId = INVALID_ORIGIN_ID;
};

}
