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

#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>

namespace NES::Windowing
{

<<<<<<<< HEAD:nes-logical-operators/include/Operators/Windows/Aggregations/MaxAggregationDescriptor.hpp
/**
 * @brief
 * The MaxAggregationDescriptor aggregation calculates the maximum over the window.
 */
class MaxAggregationDescriptor : public WindowAggregationDescriptor
{
public:
    /**
     * Factory method to create a MaxAggregationDescriptor aggregation on a particular field.
     */
    static std::shared_ptr<WindowAggregationDescriptor> on(const std::shared_ptr<NodeFunction>& onField);

    static std::shared_ptr<WindowAggregationDescriptor>
    create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField);
========
class CountAggregationFunction : public WindowAggregationFunction
{
public:
    static constexpr std::string_view NAME = "Avg";

    static std::shared_ptr<WindowAggregationFunction> on(const std::shared_ptr<LogicalFunction>& onField);

    static std::shared_ptr<WindowAggregationFunction>
    create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField);
>>>>>>>> beb1bef1ac (refactor(LogicalOperators): move & include):nes-logical-operators/include/Operators/Windows/Aggregations/CountAggregationFunction.hpp

    std::shared_ptr<DataType> getInputStamp() override;
    std::shared_ptr<DataType> getPartialAggregateStamp() override;
    std::shared_ptr<DataType> getFinalAggregateStamp() override;

    void inferStamp(const Schema& schema) override;

<<<<<<<< HEAD:nes-logical-operators/include/Operators/Windows/Aggregations/MaxAggregationDescriptor.hpp
    std::shared_ptr<WindowAggregationDescriptor> copy() override;
    MaxAggregationDescriptor(const std::shared_ptr<NodeFunction>& onField, const std::shared_ptr<NodeFunction>& asField);
========
    std::shared_ptr<WindowAggregationFunction> clone() override;
>>>>>>>> beb1bef1ac (refactor(LogicalOperators): move & include):nes-logical-operators/include/Operators/Windows/Aggregations/CountAggregationFunction.hpp

    virtual ~CountAggregationFunction() = default;

    NES::SerializableAggregationFunction serialize() const override;

private:
<<<<<<<< HEAD:nes-logical-operators/include/Operators/Windows/Aggregations/MaxAggregationDescriptor.hpp
    explicit MaxAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField);
========
    explicit CountAggregationFunction(const std::shared_ptr<FieldAccessLogicalFunction> onField);
    CountAggregationFunction(const std::shared_ptr<LogicalFunction> onField, const std::shared_ptr<LogicalFunction> asField);
>>>>>>>> beb1bef1ac (refactor(LogicalOperators): move & include):nes-logical-operators/include/Operators/Windows/Aggregations/CountAggregationFunction.hpp
};
}
