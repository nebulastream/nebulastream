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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_BASEVARIABLESIZETYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_BASEVARIABLESIZETYPE_HPP_
#include <type_traits>

namespace NES {
/**
 * @brief Base class for all nes specific variable-sized data types
 */
class BaseVariableSizeType {};

template<class Type>
concept IsVariableSizeType =
    std::is_base_of_v<BaseVariableSizeType, Type> || std::is_base_of_v<BaseVariableSizeType, std::remove_pointer_t<Type>>;

}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_BASEVARIABLESIZETYPE_HPP_
