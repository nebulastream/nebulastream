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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TYPEDREF_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TYPEDREF_HPP_
#include <Nautilus/Interface/DataTypes/BaseTypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Nautilus {

/**
 * @brief Data type to represent a reference to an underling typed pointer that is managed by Nautilus.
 */
template<typename T>
class TypedRef final : public BaseTypedRef {
  public:
    using element_type = T;

    static void destructReference(void* value) {
        auto* typedPtr = (T*) value;
        typedPtr->~T();
    }
    static const inline auto type = TypeIdentifier::create<TypedRef<T>>();
    TypedRef() : BaseTypedRef(&type), value(std::make_shared<Value<MemRef>>(Value<MemRef>(std::make_unique<MemRef>(nullptr)))){};
    TypedRef(T* t) : BaseTypedRef(&type), value(std::make_shared<Value<MemRef>>((int8_t*) t)){};

    // copy constructor
    TypedRef(const TypedRef<T>& t) : BaseTypedRef(&type), value(t.value){};

    // move constructor
    TypedRef(const TypedRef<T>&& other) : BaseTypedRef(&type), value(std::move(other.value)){};

    // copy assignment
    TypedRef<T>& operator=(const TypedRef<T>& other) { return *this = TypedRef<T>(other); };

    // move assignment
    TypedRef<T>& operator=(const TypedRef<T>&& other) {
        std::swap(value, other.value);
        return *this;
    };

    std::shared_ptr<Any> copy() override { return std::make_shared<TypedRef<T>>(*this); }
    T* get() { return reinterpret_cast<T*>(value->value->value); }
    Nautilus::IR::Types::StampPtr getType() const override { return Nautilus::IR::Types::StampFactory::createAddressStamp(); }
    std::shared_ptr<Value<MemRef>> value;
    ~TypedRef() {
        if (value.use_count() == 1) {
            FunctionCall<>("DestructTypedRef", destructReference, *value.get());
        }
    }
};

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TYPEDREF_HPP_
