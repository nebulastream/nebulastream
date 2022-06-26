#include <Experimental/NESIR/BasicBlocks/BasicBlockArgument.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
BasicBlockArgument::BasicBlockArgument(const OperationIdentifier identifier, PrimitiveStamp stamp)
    : Operation(OperationType::BasicBlockArgument,identifier, stamp) {}
std::ostream& operator<<(std::ostream& os, const BasicBlockArgument& argument) {
    os << argument.identifier;
    return os;
}
std::string BasicBlockArgument::toString() { return identifier; }
}// namespace NES::ExecutionEngine::Experimental::IR::Operations