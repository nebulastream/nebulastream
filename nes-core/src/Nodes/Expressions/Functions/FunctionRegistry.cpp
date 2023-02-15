#include <Nodes/Expressions/Functions/FunctionRegistry.hpp>
NES::DataTypePtr NES::UnaryLogicalFunction::inferStamp(const std::vector<DataTypePtr>& inputStamps) const {
    return inferUnary(inputStamps[0]);
}
