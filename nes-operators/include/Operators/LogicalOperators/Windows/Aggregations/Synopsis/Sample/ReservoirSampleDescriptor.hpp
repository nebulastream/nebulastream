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

#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>

namespace NES::Windowing
{

// TODO(nikla44): get reservoirSize from query
constexpr auto RESERVOIR_SIZE = 5;

class ReservoirSampleDescriptor final : public WindowAggregationDescriptor
{
public:
    static std::shared_ptr<WindowAggregationDescriptor> on(const std::shared_ptr<NodeFunction>& onField);

    /// Creates a new ReservoirSampleDescriptor
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    static std::shared_ptr<WindowAggregationDescriptor>
    create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField);

    [[nodiscard]] uint64_t getReservoirSize() const;

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<WindowAggregationDescriptor> copy() override;

    std::shared_ptr<DataType> getInputStamp() override;
    std::shared_ptr<DataType> getPartialAggregateStamp() override;
    std::shared_ptr<DataType> getFinalAggregateStamp() override;

    virtual ~ReservoirSampleDescriptor() = default;

private:
    explicit ReservoirSampleDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField);
    ReservoirSampleDescriptor(const std::shared_ptr<NodeFunction>& onField, const std::shared_ptr<NodeFunction>& asField);

    uint64_t reservoirSize;
};

}
