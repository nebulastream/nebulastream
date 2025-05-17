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
#include <string_view>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class WindowAggregationLogicalFunction
{
public:
    virtual ~WindowAggregationLogicalFunction() = default;

    [[nodiscard]] std::shared_ptr<DataType> getInputStamp() const;
    [[nodiscard]] std::shared_ptr<DataType> getPartialAggregateStamp() const;
    [[nodiscard]] std::shared_ptr<DataType> getFinalAggregateStamp() const;

    [[nodiscard]] std::string toString() const;
    bool operator==(const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const;

    /// @brief Infers the dataType of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema) = 0;

    [[nodiscard]] virtual SerializableAggregationFunction serialize() const = 0;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;

    std::shared_ptr<DataType> inputStamp, partialAggregateStamp, finalAggregateStamp;
    FieldAccessLogicalFunction onField, asField;

protected:
    explicit WindowAggregationLogicalFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        FieldAccessLogicalFunction onField,
        FieldAccessLogicalFunction asField);

    explicit WindowAggregationLogicalFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        const FieldAccessLogicalFunction& onField);
};
}
