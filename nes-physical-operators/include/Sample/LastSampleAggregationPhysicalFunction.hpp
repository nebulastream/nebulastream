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

#include <cstddef>
#include <cstdint>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/Record.hpp>
#include <Sample/SampleAggregationPhysicalFunction.hpp>

namespace NES
{

class LastSampleAggregationPhysicalFunction final : public SampleAggregationPhysicalFunction
{
public:
    LastSampleAggregationPhysicalFunction(
        const Schema<QualifiedUnboundField, Ordered>& stateSchema, Record::RecordFieldIdentifier timestampFieldName);

    void
    lift(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record) const override;

    void combine(
        nautilus::val<SampleAggregationState*> state1,
        nautilus::val<SampleAggregationState*> state2,
        PipelineMemoryProvider& pipelineMemoryProvider) const override;

    [[nodiscard]] Record lower(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider) const override;

    [[nodiscard]] nautilus::val<bool> isEmpty(nautilus::val<SampleAggregationState*> state) const override;

    void reset(nautilus::val<SampleAggregationState*> state, PipelineMemoryProvider& pipelineMemoryProvider) const override;

    void cleanup(nautilus::val<SampleAggregationState*> state) const override;

    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

private:
    struct FieldLayout
    {
        Record::RecordFieldIdentifier fieldIdentifier;
        DataType type;
        uint64_t offset;
    };

    std::vector<FieldLayout> fields;
    uint64_t timestampFieldOffset{0};
    DataType timestampFieldType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    uint64_t stateSize{0};
};

}
