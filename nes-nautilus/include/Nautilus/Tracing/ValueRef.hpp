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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_VALUEREF_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_VALUEREF_HPP_
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <memory>
namespace NES::Nautilus::Tracing {

/**
 * @brief References of a value with a block_id and operation_id tag
 */
class ValueRef {
  public:
    ValueRef();
    /**
     * @brief Constructor to create a new value reference
     * @param blockId
     * @param operationId
     * @param type
     */
    ValueRef(uint32_t blockId, uint32_t operationId, IR::Types::StampPtr type);

    /**
     * @brief Copy constructor
     * @param other
     */
    ValueRef(const ValueRef& other);

    /**
     * @brief Move constructor
     * @param other
     */
    ValueRef(const ValueRef&& other);

    /**
     * @brief Copy assignment
     * @param other
     * @return ValueRef
     */
    ValueRef& operator=(const ValueRef& other);

    /**
     * @brief Move assignment
     * @param rhs
     * @return ValueRef
     */
    ValueRef& operator=(const ValueRef&& other);
    bool operator==(const ValueRef& rhs) const;
    bool operator!=(const ValueRef& rhs) const;
    friend std::ostream& operator<<(std::ostream& os, const ValueRef& tag);

    uint32_t blockId;
    uint32_t operationId;
    IR::Types::StampPtr type;
};

/**
 * @brief Utility function to create the next value reference.
 * @param type
 * @return ValueRef
 */
ValueRef createNextRef(const IR::Types::StampPtr& type);

struct ValueRefHasher {
    std::size_t operator()(const ValueRef& k) const {
        auto hasher = std::hash<uint64_t>();
        std::size_t hashVal = hasher(k.operationId) ^ hasher(k.blockId);
        return hashVal;
    }
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_VALUEREF_HPP_
