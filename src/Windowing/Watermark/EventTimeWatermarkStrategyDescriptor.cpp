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

#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(ExpressionItem onField, TimeMeasure delay) : onField(onField), delay(delay) {
}

WatermarkStrategyDescriptorPtr EventTimeWatermarkStrategyDescriptor::create(ExpressionItem onField, TimeMeasure delay) {
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(Windowing::EventTimeWatermarkStrategyDescriptor(onField, delay));
}
ExpressionItem EventTimeWatermarkStrategyDescriptor::getOnField() {
    return onField;
}
TimeMeasure EventTimeWatermarkStrategyDescriptor::getDelay() {
    return delay;
}
bool EventTimeWatermarkStrategyDescriptor::equal(WatermarkStrategyDescriptorPtr other) {
    auto eventTimeWatermarkStrategyDescriptor = other->as<EventTimeWatermarkStrategyDescriptor>();
    return eventTimeWatermarkStrategyDescriptor->onField.getExpressionNode() == onField.getExpressionNode() && eventTimeWatermarkStrategyDescriptor->delay.getTime() == delay.getTime();
}
}// namespace NES::Windowing