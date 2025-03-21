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
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowOperator.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES
{

class WindowedAggregationLogicalOperator final : public WindowOperator
{
public:
    WindowedAggregationLogicalOperator(std::vector<std::unique_ptr<FieldAccessLogicalFunction>> onKey,
                          std::vector<std::unique_ptr<WindowAggregationLogicalFunction>> windowAggregation,
                          std::unique_ptr<Windowing::WindowType> windowType);
    virtual ~WindowedAggregationLogicalOperator() = default;

    std::string_view getName() const noexcept override;

    [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override;
    //bool inferSchema() override;

    std::vector<std::string> getGroupByKeyNames() const;

    bool isKeyed() const;

    [[nodiscard]] const std::vector<std::unique_ptr<WindowAggregationLogicalFunction>>& getWindowAggregation() const;
    void setWindowAggregation(std::vector<std::unique_ptr<WindowAggregationLogicalFunction>> windowAggregation);

    [[nodiscard]] Windowing::WindowType& getWindowType() const;
    void setWindowType(std::unique_ptr<Windowing::WindowType> windowType);

    [[nodiscard]] const std::vector<std::unique_ptr<FieldAccessLogicalFunction>>& getKeys() const;
    void setOnKey(std::vector<std::unique_ptr<FieldAccessLogicalFunction>> onKeys);

    [[nodiscard]] OriginId getOriginId() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

    [[nodiscard]] std::string getWindowStartFieldName() const;
    [[nodiscard]] std::string getWindowEndFieldName() const;

    [[nodiscard]] SerializableOperator serialize() const override;
    std::string toString() const override;

    std::vector<Schema> getInputSchemas() const override { return {inputSchema}; };
    Schema getOutputSchema() const override { return outputSchema; }
    std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    std::vector<OriginId> getOutputOriginIds() const override { return {}; }

private:
    static constexpr std::string_view NAME = "WindowedAggregation";
    std::vector<std::unique_ptr<WindowAggregationLogicalFunction>> windowAggregation;
    std::unique_ptr<Windowing::WindowType> windowType;
    std::vector<std::unique_ptr<FieldAccessLogicalFunction>> onKey;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId = INVALID_ORIGIN_ID;
    Schema inputSchema, outputSchema;
};

}
