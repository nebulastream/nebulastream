#include <Experimental/NESIR/BasicBlocks/BasicBlockArgument.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
BasicBlockArgument::BasicBlockArgument(const std::string identifier, PrimitiveStamp stamp)
    : Operation(OperationType::BasicBlockArgument, stamp), identifier(identifier) {}
std::ostream& operator<<(std::ostream& os, const BasicBlockArgument& argument) {
    os << argument.identifier;
    return os;
}
const std::string& BasicBlockArgument::getIdentifier() const { return identifier; }
void BasicBlockArgument::setIdentifier(const std::string& identifier) { BasicBlockArgument::identifier = identifier; }
std::string BasicBlockArgument::toString() { return identifier; }
}// namespace NES::ExecutionEngine::Experimental::IR::Operations