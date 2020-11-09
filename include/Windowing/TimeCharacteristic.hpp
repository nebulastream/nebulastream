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

#ifndef NES_INCLUDE_WINDOWING_TIMECHARACTERISTIC_HPP_
#define NES_INCLUDE_WINDOWING_TIMECHARACTERISTIC_HPP_

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {
/**
 * @brief The time stamp characteristic represents if an window is in event or processing time.
 */
class TimeCharacteristic {
  public:
    /**
     * @brief The type as enum.
     */
    enum Type {
        ProcessingTime,
        EventTime
    };
    explicit TimeCharacteristic(Type type);
    TimeCharacteristic(Type type, AttributeFieldPtr field);

    /**
     * @brief Factory to create a time characteristic for processing time window
     * @return TimeCharacteristicPtr
     */
    static TimeCharacteristicPtr createProcessingTime();

    /**
     * @brief Factory to create a event time window with an time extractor on a specific field.
     * @param field the field from which we want to extract the time.
     * @return
     */
    static TimeCharacteristicPtr createEventTime(ExpressionItem field);

    /**
     * @return The TimeCharacteristic type.
     */
    Type getType();

    /**
     * @return  If it is a event time window this returns the field, from which we extract the time stamp.
     */
    AttributeFieldPtr getField();

  private:
    Type type;
    AttributeFieldPtr field;
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_TIMECHARACTERISTIC_HPP_
