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
#include <Functions/NodeFunction.hpp>
#include <Measures/TimeUnit.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>

namespace NES::Windowing
{


class EventTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor
{
public:
    static std::shared_ptr<WatermarkStrategyDescriptor> create(const std::shared_ptr<NodeFunction>& onField, TimeUnit unit);

    std::shared_ptr<NodeFunction> getOnField() const;

    void setOnField(const std::shared_ptr<NodeFunction>& newField);

    TimeUnit getTimeUnit() const;

    void setTimeUnit(const TimeUnit& newUnit);

    bool equal(std::shared_ptr<WatermarkStrategyDescriptor> other) override;

    std::string toString() override;

    bool inferStamp(const std::shared_ptr<Schema>& schema) override;

private:
    /// Field where the watermark should be retrieved
    std::shared_ptr<NodeFunction> onField;
    TimeUnit unit;

    explicit EventTimeWatermarkStrategyDescriptor(std::shared_ptr<NodeFunction> onField, TimeUnit unit);
};

}
