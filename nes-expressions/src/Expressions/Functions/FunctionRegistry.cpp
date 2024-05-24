/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Expressions/Functions/LogicalFunctionRegistry.hpp>

namespace NES {

DataTypePtr UnaryLogicalFunction::inferStamp(const std::vector<DataTypePtr>& inputStamps) const {
    NES_ASSERT(inputStamps.size() == 1, "Unary function should only receive one input stamp.");
    return inferUnary(inputStamps[0]);
}

DataTypePtr BinaryLogicalFunction::inferStamp(const std::vector<DataTypePtr>& inputStamps) const {
    NES_ASSERT(inputStamps.size() == 2, "Binary function should receive two input stamp.");
    return inferBinary(inputStamps[0], inputStamps[1]);
}

}// namespace NES
