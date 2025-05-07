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

#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <LogicalFunctions/Function.hpp>

namespace NES::Logical
{

class WindowAggregationFunction
{
public:
    virtual ~WindowAggregationFunction() = default;

    std::shared_ptr<DataType> getInputStamp() const;
    std::shared_ptr<DataType> getPartialAggregateStamp() const;
    std::shared_ptr<DataType> getFinalAggregateStamp() const;

    std::string toString() const;
    bool operator==(std::shared_ptr<WindowAggregationFunction> otherWindowAggregationFunction) const;

    /// @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema) = 0;

    virtual SerializableAggregationFunction serialize() const = 0;

    virtual std::string_view getName() const noexcept = 0;

    std::shared_ptr<DataType> inputStamp, partialAggregateStamp, finalAggregateStamp;
    FieldAccessFunction onField, asField;

protected:
    explicit WindowAggregationFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        FieldAccessFunction onField,
        FieldAccessFunction asField);

    explicit WindowAggregationFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        FieldAccessFunction onField);
};
}
