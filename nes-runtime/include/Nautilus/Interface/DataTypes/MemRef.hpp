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

template<typename T>
class TypedMemRef : public TraceableType {
  public:
    static const inline auto type = TypeIdentifier::create<TypedMemRef<T>>();
    using RawType = T*;
    static constexpr auto TypeSize = sizeof(T);
    explicit TypedMemRef(T* ptr) : TraceableType(&type), ptr(ptr){};
    TypedMemRef(TypedMemRef&& a) : TypedMemRef(a.ptr) {}
    TypedMemRef(TypedMemRef& a) : TypedMemRef(a.ptr) {}
   TypedMemRef<T> add(Int8& otherValue) const {
        auto val1 = ptr + otherValue.getValue();
        return val1;
    };

    std::shared_ptr<Any> copy() override { return std::make_unique<TypedMemRef<T>>(this->ptr); }

    auto getRaw(){
        return ptr;
    }

    void store(Any&) {

    }

  private:
    T* ptr;
};


using MemRef = TypedMemRef<int8_t>;


}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_MEMREF_HPP_
