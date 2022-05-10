#include <Interpreter/Trace/Block.hpp>

namespace NES::Interpreter {

bool Block::isLocalValueRef(ValueRef& ref) {

    for (const auto& operation : operations) {
        if (auto resultValueRef = std::get_if<ValueRef>(&operation.result)) {
            if (resultValueRef->blockId == ref.blockId && resultValueRef->operationId == ref.operationId) {
                return true;
            }
        }
    }
    //if (ref.blockId == blockId) {
    // this is a local ref
    //    return true;
    //}
    return std::find(arguments.begin(), arguments.end(), ref) != arguments.end();
}

void Block::addArgument(ValueRef ref) {
    // only add ref to arguments if it not already exists
    if (std::find(arguments.begin(), arguments.end(), ref) == arguments.end()) {
        arguments.emplace_back(ref);
    }
}


std::ostream& operator<<(std::ostream& os, const Block& block) {
    os << "(";
    for (size_t i = 0; i < block.arguments.size(); i++) {
        os << block.arguments[i] << ",";
    }
    os << ")\n";
    for (size_t i = 0; i < block.operations.size(); i++) {
        os << "\t" << block.operations[i] << "\n";
    }
    return os;
}

}// namespace NES::Interpreter