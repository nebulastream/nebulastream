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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREF_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREF_HPP_
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
namespace NES::Nautilus {

/**
 * @brief Data type to represent a memory location.
 */
class MemRef : public TraceableType {
  public:
    static const inline auto type = TypeIdentifier::create<MemRef>();
    using RawType = int8_t*;
    MemRef(int8_t* value) : TraceableType(&type), value(value){};
    MemRef(MemRef&& a) : MemRef(a.value) {}
    MemRef(MemRef& a) : MemRef(a.value) {}
    std::shared_ptr<Any> copy() override { return std::make_unique<MemRef>(this->value); }

    std::shared_ptr<MemRef> add(Int8& otherValue) const {
        auto val1 = value + otherValue.getValue();
        return std::make_unique<MemRef>(val1);
    };
    ~MemRef() {}

    void* getValue() { return value; }

    template<class ResultType>
    std::shared_ptr<ResultType> load() {
        auto typedAddress = reinterpret_cast<typename ResultType::RawType*>(value);
        return std::make_unique<ResultType>(*typedAddress);
    }

    /**
     * @brief Stores a value to a memory location.
     * @tparam T
     * @param valueType
     */
    template<typename T>
    void store(Any& valueType) {
        *reinterpret_cast<typename T::RawType*>(value) = valueType.staticCast<T>().getValue();
    }

    /**
     * @brief Stores a value to the pointer in the memref.
     * Depending on the value type we determine the correct method to store it.
     * @param valueType
     */
    void store(Any& valueType) {
        if (valueType.isType<Int8>()) {
            store<Int8>(valueType);
        } else if (valueType.isType<Int16>()) {
            store<Int16>(valueType);
        } else if (valueType.isType<Int32>()) {
            store<Int32>(valueType);
        } else if (valueType.isType<Int64>()) {
            store<Int64>(valueType);
        } else if (valueType.isType<UInt8>()) {
            store<UInt8>(valueType);
        } else if (valueType.isType<UInt16>()) {
            store<UInt16>(valueType);
        } else if (valueType.isType<UInt32>()) {
            store<UInt32>(valueType);
        } else if (valueType.isType<UInt64>()) {
            store<UInt64>(valueType);
        } else if (valueType.isType<Float>()) {
            store<Float>(valueType);
        } else if (valueType.isType<Double>()) {
            store<Double>(valueType);
        } else if (valueType.isType<Boolean>()) {
            store<Boolean>(valueType);
        }
    }

    Nautilus::IR::Types::StampPtr getType() const override { return Nautilus::IR::Types::StampFactory::createAddressStamp(); }
    int8_t* value;
};

}// namespace NES::Nautilus

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREF_HPP_
