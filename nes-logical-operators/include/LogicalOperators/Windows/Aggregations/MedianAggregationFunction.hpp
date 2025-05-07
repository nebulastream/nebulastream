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
#include <LogicalFunctions/Function.hpp>

namespace NES::Logical
{

class MedianAggregationFunction : public WindowAggregationFunction
{
public:
    MedianAggregationFunction(FieldAccessFunction onField, FieldAccessFunction asField);
    static std::shared_ptr<WindowAggregationFunction> create(FieldAccessFunction onField);
    explicit MedianAggregationFunction(const FieldAccessFunction& onField);

    /// Creates a new MedianAggregationFunction
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    static std::shared_ptr<WindowAggregationFunction>
    create(const FieldAccessFunction& onField, const FieldAccessFunction& asField);

    void inferStamp(const Schema& schema) override;

    virtual ~MedianAggregationFunction() = default;

    NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;


private:
    static constexpr std::string_view NAME = "Median";
};
}
