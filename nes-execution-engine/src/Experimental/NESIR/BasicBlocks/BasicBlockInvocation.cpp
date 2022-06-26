#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

BasicBlockInvocation::BasicBlockInvocation() {}

void BasicBlockInvocation::setBlock(BasicBlockPtr block) { this->basicBlock = block; }

BasicBlockPtr BasicBlockInvocation::getBlock() { return basicBlock; }

void BasicBlockInvocation::addArgument(OperationPtr argument) { this->operations.emplace_back(argument); }

void BasicBlockInvocation::removeArgument(uint64_t argumentIndex) {
    operations.erase(operations.begin() + argumentIndex);
}

std::vector<OperationPtr> BasicBlockInvocation::getArguments() {
    std::vector<OperationPtr> arguments;
    for (auto& arg : this->operations) {
        arguments.emplace_back(arg.lock());
    }
    return arguments;
}

}// namespace NES::ExecutionEngine::Experimental::IR::Operations