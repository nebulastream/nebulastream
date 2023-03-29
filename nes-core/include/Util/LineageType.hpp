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

#ifndef NES_CORE_INCLUDE_UTIL_LINEAGETYPE_HPP_
#define NES_CORE_INCLUDE_UTIL_LINEAGETYPE_HPP_

namespace NES {

enum class LineageType : uint8_t {
    NONE = 0,      /// no lineage
    IN_MEMORY = 1, /// lineage is stored in memory on nodes
    PERSISTENT = 2,/// lineage is stored in persistent memory
    REMOTE = 3,    /// lineage is stored in remote storage
    INVALID = 4
};
}// namespace NES
#endif// NES_CORE_INCLUDE_UTIL_LINEAGETYPE_HPP_
