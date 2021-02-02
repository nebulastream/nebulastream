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

#ifndef NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEUNIT_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEUNIT_HPP_
#include <Windowing/WindowMeasures/WindowMeasure.hpp>
#include <cstdint>
namespace NES::Windowing {

/**
 * A time based window measure.
 */
class TimeUnit : public WindowMeasure {
  public:
    explicit TimeUnit(uint64_t multiplier);

    /**
     * @brief gets the multiplier to convert this to milliseconds
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getMultiplier() const;

    std::string toString() override;

  private:
    const uint64_t multiplier;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEUNIT_HPP_
