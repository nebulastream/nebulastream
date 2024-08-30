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

#pragma once

#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus
{

/**
* @brief Get member returns the MemRef to a specific class member as an offset to a objectReference.
* @note This assumes the offsetof works for the classType.
* @param objectReference reference to the object that contains the member.
* @param classType type of a class or struct
* @param member a member that is part of the classType
*/
#define getMember(objectReference, classType, member, dataType) \
    (static_cast<nautilus::val<dataType>>( \
        *static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member))))))
#define getMemberAsVarVal(objectReference, classType, member, dataType) \
    VarVal(getMember(objectReference, classType, member, dataType), BooleanVal(false))
#define getMemberAsPointer(objectReference, classType, member, dataType) \
    (static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member)))))

#define writeValueToMemRef(memRef, value, dataType) (*static_cast<nautilus::val<dataType*>>(memRef) = value)
#define readValueFromMemRef(memRef, dataType) (static_cast<nautilus::val<dataType>>(*static_cast<nautilus::val<dataType*>>(memRef)))

}
