#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

Any::Any(const TypeIdentifier* identifier) : Typed(identifier){};

IR::Types::StampPtr Any::getType() const { return IR::Types::StampFactory::createVoidStamp(); }

}// namespace NES::ExecutionEngine::Experimental::Interpreter