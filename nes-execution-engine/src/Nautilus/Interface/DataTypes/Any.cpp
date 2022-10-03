#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>

namespace NES::Nautilus {

Any::Any(const TypeIdentifier* identifier) : Typed(identifier){};

Nautilus::IR::Types::StampPtr Any::getType() const { return Nautilus::IR::Types::StampFactory::createVoidStamp(); }

}// namespace NES::Nautilus