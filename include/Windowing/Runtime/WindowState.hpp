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

#ifndef NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_

#include <cstdint>

namespace NES::Windowing {

/**
 * @brief Represents the start and end of a single window
 */
class WindowState {

  public:
    WindowState(uint64_t start, uint64_t end);

    /**
     * @brief Returns the window start.
     * @return uint64_t
     */
    uint64_t getStartTs() const;
    /**
    * @brief Returns the window end.
    * @return uint64_t
    */
    uint64_t getEndTs() const;

  private:
    uint64_t start;
    uint64_t end;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWSTATE_HPP_
