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

#ifndef NES_EVENTTIMEWATERMARKSTRATEGY_HPP
#define NES_EVENTTIMEWATERMARKSTRATEGY_HPP

#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES::Windowing {

class EventTimeWatermarkStrategy;
typedef std::shared_ptr<EventTimeWatermarkStrategy> EventTimeWatermarkStrategyPtr;

class EventTimeWatermarkStrategy : public WatermarkStrategy {
  public:
    EventTimeWatermarkStrategy(FieldAccessExpressionNodePtr onField, uint64_t delay);

    FieldAccessExpressionNodePtr getField();

    uint64_t getAllowedLateness();

    Type getType() override;

    static EventTimeWatermarkStrategyPtr create(FieldAccessExpressionNodePtr onField, uint64_t delay);

  private:
    // Field where the watermark should be retrieved
    FieldAccessExpressionNodePtr onField;

    // Watermark delay
    uint64_t delay;
};

}// namespace NES::Windowing

#endif//NES_EVENTTIMEWATERMARKSTRATEGY_HPP
