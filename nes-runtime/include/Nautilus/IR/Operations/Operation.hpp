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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_

#include <Nautilus/IR/Types/BasicTypes.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <memory>
#include <vector>

namespace NES::Nautilus::IR::Types {
class Stamp;
using StampPtr = std::shared_ptr<Stamp>;
}// namespace NES::Nautilus::IR::Types

namespace NES::Nautilus::IR::Operations {
using OperationIdentifier = std::string;
class Operation {
  public:
    enum class ProxyCallType : uint8_t { GetNumTuples = 0, SetNumTuples = 1, GetDataBuffer = 2, Other = 50 };
    enum class OperationType : uint8_t {
        AddOp,
        AddressOp,
        AndOp,
        BitWiseAnd,
        BitWiseOr,
        BitWiseXor,
        BitWiseLeftShift,
        BitWiseRightShift,
        BasicBlockArgument,
        BlockInvocation,
        BranchOp,
        ConstIntOp,
        ConstBooleanOp,
        ConstFloatOp,
        CastOp,
        CompareOp,
        DivOp,
        ModOp,
        FunctionOp,
        IfOp,
        LoopOp,
        LoadOp,
        MulOp,
        MLIR_YIELD,
        NegateOp,
        OrOp,
        ProxyCallOp,
        ReturnOp,
        StoreOp,
        SubOp,
        ConstAddressOp,
        BuiltinVariableOp,
    };

    explicit Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp);
    explicit Operation(OperationType opType, Types::StampPtr stamp);
    virtual ~Operation() = default;
    OperationIdentifier getIdentifier();
    virtual std::string toString() = 0;
    OperationType getOperationType() const;
    const Types::StampPtr& getStamp() const;
    void addUsage(const Operation*);
    const std::vector<const Operation*>& getUsages();

  protected:
    OperationType opType;
    OperationIdentifier identifier;
    const Types::StampPtr stamp;
    std::vector<const Operation*> usages;
};
using OperationPtr = std::shared_ptr<Operation>;
using OperationWPtr = std::weak_ptr<Operation>;
using OperationRawPtr = Operation*;

}// namespace NES::Nautilus::IR::Operations
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_OPERATION_HPP_
