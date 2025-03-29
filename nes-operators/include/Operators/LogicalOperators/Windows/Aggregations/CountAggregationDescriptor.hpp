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
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES::Windowing
{

class CountAggregationDescriptor : public WindowAggregationDescriptor
{
public:

    static std::shared_ptr<WindowAggregationDescriptor> on(const std::shared_ptr<LogicalFunction>& keyFunction);

    static std::shared_ptr<WindowAggregationDescriptor>
    create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField);

    std::shared_ptr<DataType> getInputStamp() override;
    std::shared_ptr<DataType> getPartialAggregateStamp() override;
    std::shared_ptr<DataType> getFinalAggregateStamp() override;

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<WindowAggregationDescriptor> clone() override;

    virtual ~CountAggregationDescriptor() = default;

private:
    explicit CountAggregationDescriptor(const std::shared_ptr<FieldAccessLogicalFunction> onField);
    CountAggregationDescriptor(const std::shared_ptr<LogicalFunction> onField, const std::shared_ptr<LogicalFunction> asField);
};
}
