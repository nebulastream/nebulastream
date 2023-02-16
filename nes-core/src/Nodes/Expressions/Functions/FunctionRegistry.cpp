#include <Nodes/Expressions/Functions/FunctionRegistry.hpp>

namespace NES {

std::unique_ptr<LogicalFunction> FunctionRegistry::getFunction(const std::string& name) {
    if (!hasFunction(name)) {
        NES_THROW_RUNTIME_ERROR("Function for name " << name << " is not registered in registry.");
    }
    return registry::getPlugin(name)->create();
}

bool FunctionRegistry::hasFunction(const std::string& name) { return registry::hasPlugin(name); }

DataTypePtr UnaryLogicalFunction::inferStamp(const std::vector<DataTypePtr>& inputStamps) const {
    NES_ASSERT(inputStamps.size() == 1, "Unary function should only receive one input stamp.");
    return inferUnary(inputStamps[0]);
}
}// namespace NES
