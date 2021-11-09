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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_NESTYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_NESTYPE_HPP_

namespace NES {
/**
 * @brief Base class for all nes specific data types
 */
class NESType {};

template<class Type>
concept IsNesType =
    std::is_fundamental<Type>::value || std::is_fundamental<std::remove_pointer_t<Type>>::value || std::is_base_of_v<NESType,
                                                                                                                     Type>;
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLETYPES_NESTYPE_HPP_
