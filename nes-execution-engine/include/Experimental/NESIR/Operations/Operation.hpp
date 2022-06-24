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

#ifndef NES_OPERATION_HPP
#define NES_OPERATION_HPP

#include <Experimental/NESIR/Types/BasicTypes.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

class Operation {
  public:
    enum ProxyCallType { GetNumTuples = 0, SetNumTuples = 1, GetDataBuffer = 2, Other = 50 };
    enum OperationType {
        LoopOp,
        AddOp,
        LoadOp,
        StoreOp,
        ConstIntOp,
        ConstFloatOp,
        AddressOp,
        FunctionOp,
        BranchOp,
        IfOp,
        CompareOp,
        ReturnOp,
        ProxyCallOp,
        DivOp,
        MulOp,
        SubOp,
        OrOp,
        AndOp,
        NegateOp,
        BasicBlockArgument
    };

    explicit Operation(OperationType opType, PrimitiveStamp stamp);
    virtual ~Operation() = default;

    virtual std::string toString() = 0;
    OperationType getOperationType() const;
    PrimitiveStamp getStamp() const;

  private:
    OperationType opType;
    PrimitiveStamp stamp;
};
using OperationPtr = std::shared_ptr<Operation>;
using OperationWPtr = std::weak_ptr<Operation>;

}// namespace NES::ExecutionEngine::Experimental::IR::Operations
#endif//NES_OPERATION_HPP
