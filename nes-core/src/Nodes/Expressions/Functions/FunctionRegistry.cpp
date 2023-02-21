#include <Nodes/Expressions/Functions/FunctionRegistry.hpp>

namespace NES {

DataTypePtr UnaryLogicalFunction::inferStamp(const std::vector<DataTypePtr>& inputStamps) const {
    NES_ASSERT(inputStamps.size() == 1, "Unary function should only receive one input stamp.");
    return inferUnary(inputStamps[0]);
}
}// namespace NES
