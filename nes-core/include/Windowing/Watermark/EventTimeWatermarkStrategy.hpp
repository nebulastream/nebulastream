/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGY_HPP_
#define NES_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGY_HPP_

#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES::Windowing {

class EventTimeWatermarkStrategy;
using EventTimeWatermarkStrategyPtr = std::shared_ptr<EventTimeWatermarkStrategy>;

class EventTimeWatermarkStrategy : public WatermarkStrategy {
  public:
    EventTimeWatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t allowedLateness, uint64_t multiplier);
    ~EventTimeWatermarkStrategy() noexcept override = default;

    FieldAccessExpressionNodePtr getField();

    uint64_t getAllowedLateness() const;

    uint64_t getMultiplier() const;

    Type getType() override;

    static EventTimeWatermarkStrategyPtr
    create(const FieldAccessExpressionNodePtr& onField, uint64_t allowedLateness, uint64_t multiplier);

  private:
    // Field where the watermark should be retrieved
    FieldAccessExpressionNodePtr onField;
    uint64_t multiplier;
    uint64_t allowedLateness;
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGY_HPP_
