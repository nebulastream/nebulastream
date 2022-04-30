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

#include <Experimental/NESIR/Operations/LoadOperation.hpp>

namespace NES {

LoadOperation::LoadOperation(const uint64_t fieldIdx)
    : Operation(OperationType::LoadOp), fieldIdx(fieldIdx) {}

bool NES::LoadOperation::classof(const NES::Operation* Op) { return Op->getOperationType() == OperationType::LoadOp; }
uint64_t LoadOperation::getFieldIdx() const { return fieldIdx; }

}// namespace NES
