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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_BASICTYPES_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_BASICTYPES_HPP_

#include <stdint.h>

namespace NES::Nautilus::IR::Operations {

enum class PrimitiveStamp : uint8_t {
    //BasicTypes
    // Type < 5 is INT
    INT1 = 0,
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,
    // Type < 10 is UINT
    UINT1 = 5,
    UINT8 = 6,
    UINT16 = 7,
    UINT32 = 8,
    UINT64 = 9,

    // Type < 12 is Float
    FLOAT = 10,
    DOUBLE = 11,

    BOOLEAN = 12,
    CHAR = 13,
    VOID = 14,

    // Pointer Types
    INT8PTR = 15,

    //DerivedTypes
    ARRAY = 32,
    CHARARRAY = 33,
    STRUCT = 34
};

}
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_BASICTYPES_HPP_
