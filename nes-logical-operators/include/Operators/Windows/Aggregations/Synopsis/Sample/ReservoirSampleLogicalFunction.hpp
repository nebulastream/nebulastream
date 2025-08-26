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

#include <cstdint>
#include <memory>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

class ReservoirSampleLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    /// This function creates a ReservoirSampleLogicalFunction even though the name doesn't make sense
    static std::shared_ptr<WindowAggregationLogicalFunction> create(const FieldAccessLogicalFunction& onField, std::vector<FieldAccessLogicalFunction> sampleFields, uint64_t reservoirSize);
    /// `asField` used when the reservoir should be renamed in the query
    /// `reservoirSize` number of records the reservoir should hold
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, std::vector<FieldAccessLogicalFunction> sampleFields, uint64_t reservoirSize);

    /// TODO The onField is unused and should be removed.
    ReservoirSampleLogicalFunction(const FieldAccessLogicalFunction& onField, std::vector<FieldAccessLogicalFunction> sampleFields, uint64_t reservoirSize);
    ReservoirSampleLogicalFunction(
        const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, std::vector<FieldAccessLogicalFunction> sampleFields, uint64_t reservoirSize);

    void inferStamp(const Schema& schema) override;
    ~ReservoirSampleLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    [[nodiscard]] uint64_t getReservoirSize() const;
    [[nodiscard]] std::vector<FieldAccessLogicalFunction> getSampleFields() const;

private:
    /// Selects which fields get projected into the sample.
    std::vector<FieldAccessLogicalFunction> sampleFields;
    uint64_t reservoirSize;

    static constexpr std::string_view NAME = "ReservoirSample";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
};

}
