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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREFUTILS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREFUTILS_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory>
namespace NES {
class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;
}// namespace NES

namespace NES::Nautilus::MemRefUtils {

/**
 * @brief Loads a value from a memref according to a physical type.
 * @param ptr to the address.
 * @param dataType physical data type.
 * @return Value that was loaded.
 */
Value<> loadValue(Value<MemRef>& ptr, const PhysicalTypePtr& dataType);

/**
 * @brief Compares the memory from ptr1 and ptr2 for size bytes.
 * @param ptr1 ptr to the first memory location.
 * @param ptr2 ptr to the second memory location.
 * @param size
 * @return true if both are the same.
 */
bool memEquals(Value<MemRef>&& ptr1, Value<MemRef>&& ptr2, Value<UInt64>&& size);

/**
 * @brief Performs a mem copy of size bytes from the source to the destination.
 * @param destination
 * @param source
 * @param size
 */
void memCopy(Value<MemRef>&& destination, Value<MemRef>&& source, Value<UInt64>&& size);

/**
 * @brief Get member returns the MemRef to a specific class member as an offset to a objectReference.
 * @note This assumes the offsetof works for the classType.
 * @param objectReference reference to the object that contains the member.
 * @param classType type of a class or struct
 * @param member a member that is part of the classType
 */
#define getMember(objectReference, classType, member)                                                                            \
    (objectReference + ((uint64_t) __builtin_offsetof(classType, member))).as<NES::Nautilus::MemRef>()

}// namespace NES::Nautilus::MemRefUtils
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREFUTILS_HPP_
