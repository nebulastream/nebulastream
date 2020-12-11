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

#ifndef NES_INCLUDE_WINDOWING_WINDOWTYPES_TUMBLINGWINDOW_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWTYPES_TUMBLINGWINDOW_HPP_

#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {
/**
 * A TumblingWindow assigns records to non-overlapping windows.
 */
class TumblingWindow : public WindowType {
  public:
    /**
    * Creates a new TumblingWindow that assigns
    * elements to time windows based on the element timestamp and multiplier.
    * For example, if you want window a stream by hour,but window begins at the 15th minutes
    * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
    * time windows start at 0:15:00,1:15:00,2:15:00,etc.
    * @param size
    * @return WindowTypePtr
    */
    static WindowTypePtr of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size);

    /**
    * Calculates the next window end based on a given timestamp.
    * The next window end is equal to the current timestamp plus the window size minus the fraction of the current window
    * @param currentTs
    * @return the next window end
    */
    [[nodiscard]] uint64_t calculateNextWindowEnd(uint64_t currentTs) const override;

    /**
    * @brief Returns true, because this a tumbling window
    * @return true
    */
    bool isTumblingWindow() override;

    /**
    * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
    * @param windows vector of windows
    * @param lastWatermark
    * @param currentWatermark
    */
    void triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize();

    std::string toString() override;

  private:
    TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size);
    const TimeMeasure size;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWTYPES_TUMBLINGWINDOW_HPP_
