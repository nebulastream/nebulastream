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

#include <Execution/Operators/MEOS/Meos.hpp>

namespace MEOS {

    /**
     * @brief Initialize MEOS library
     * @param[in] timezone Timezone of reference
     * @note The second parameter refers to the error handler, always set to NULL
     */
    Meos::Meos(std::string timezone) { meos_initialize(timezone.c_str(), NULL); }

    /**
     * @brief Finalize MEOS library, free the timezone cache
     */
    Meos::~Meos() { meos_finalize(); }
}// namespace MEOS
