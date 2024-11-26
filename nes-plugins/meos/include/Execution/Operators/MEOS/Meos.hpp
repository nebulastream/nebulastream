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

#ifndef NES_PLUGINS_MEOS_INCLUDE_EXECUTION_OPERATORS_MEOS_MEOS_HPP_
#define NES_PLUGINS_MEOS_INCLUDE_EXECUTION_OPERATORS_MEOS_MEOS_HPP_
#include <string>

namespace MEOS {
extern "C" {
#include <meos.h>
}

/**
 * @brief MEOS class encapsulates the initialization and finalization of the MEOS library. 
 * It automatically initializes the library with a specified timezone when an instance is created and 
 * ensures proper resource cleanup upon destruction. 
 */
class Meos {
  public:
    /**
     * @brief Initialize MEOS library
     * @param[in] timezone Timezone of reference
     * @note The second parameter refers to the error handler, always set to NULL
     */
    Meos(std::string);

    /**
     * @brief Finalize MEOS library, free the timezone cache
     */
    ~Meos();
};
}// namespace MEOS

#endif// NES_PLUGINS_MEOS_INCLUDE_EXECUTION_OPERATORS_MEOS_MEOS_HPP_
