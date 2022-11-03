
#include <Nautilus/Interface/DataTypes/Any.hpp>

#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BASETYPEDREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BASETYPEDREF_HPP_
namespace NES::Nautilus {

class BaseTypedRef : public Any {
  public:
    BaseTypedRef(const TypeIdentifier* identifier) : Any(identifier){};
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BASETYPEDREF_HPP_
