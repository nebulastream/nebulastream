//
// Created by pgrulich on 16.05.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_VECTOR_REF_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_VECTOR_REF_HPP_
#include "Nautilus/Interface/DataTypes/MemRef.hpp"
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
namespace std {
template<typename T>
class vector_ref {

    template<class T>
    NES::Nautilus::Value<NES::Nautilus::MemRef> inline operator[](Value<T>& index) {
       ref.
    };

  private:
    NES::Nautilus::TypedRef<std::vector<T>> ref;
};
}// namespace std
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_VECTOR_REF_HPP_
