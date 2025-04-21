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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <DataType.hpp>
namespace NES::Windowing
{

class AvgAggregationDescriptor : public WindowAggregationDescriptor
{
public:
    static std::shared_ptr<WindowAggregationDescriptor> create(ExpressionValue onField, Schema::Identifier asField);

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<WindowAggregationDescriptor> copy() override;

    virtual ~AvgAggregationDescriptor() = default;

private:
    AvgAggregationDescriptor(ExpressionValue onField, Schema::Identifier asField);
};
}
