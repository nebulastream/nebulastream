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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_IRLOWERINGINTERFACE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_IRLOWERINGINTERFACE_HPP_

#include <Nautilus/CodeGen/CodeGenerator.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/ModOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseAndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseLeftShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseOrOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseRightShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseXorOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Nautilus/Util/Frame.hpp>

#include <memory>

namespace NES::Nautilus::CodeGen {

/**
 * @brief This class defines an interface for generating code from NES IR for a back-end.
 */
class IRLoweringInterface {
public:
    using RegisterFrame = Frame<std::string, std::string>;

    /**
     * @brief This method lowers an arbitrary IR operation to a code generator.
     * @param operation the IR operation to lower
     * @param frame the register frame
     * @return A `std::unique_ptr<CodeGenerator>` of the lowered IR operation
     */
    std::unique_ptr<CodeGenerator> lowerOperation(const std::shared_ptr<IR::Operations::Operation>& operation, RegisterFrame& frame);

private:
    virtual std::unique_ptr<CodeGenerator> lowerAdd(const std::shared_ptr<IR::Operations::AddOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerAddress(const std::shared_ptr<IR::Operations::AddressOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerAnd(const std::shared_ptr<IR::Operations::AndOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBasicBlockArgument(const std::shared_ptr<IR::Operations::BasicBlockArgument>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBasicBlockInvocation(const std::shared_ptr<IR::Operations::BasicBlockInvocation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBitwiseAnd(const std::shared_ptr<IR::Operations::BitWiseAndOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBitwiseLeftShift(const std::shared_ptr<IR::Operations::BitWiseLeftShiftOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBitwiseOr(const std::shared_ptr<IR::Operations::BitWiseOrOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBitwiseRightShift(const std::shared_ptr<IR::Operations::BitWiseRightShiftOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBitwiseXor(const std::shared_ptr<IR::Operations::BitWiseXorOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerBranch(const std::shared_ptr<IR::Operations::BranchOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerCast(const std::shared_ptr<IR::Operations::CastOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerCompare(const std::shared_ptr<IR::Operations::CompareOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerConstBoolean(const std::shared_ptr<IR::Operations::ConstBooleanOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerConstFloat(const std::shared_ptr<IR::Operations::ConstFloatOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerConstInt(const std::shared_ptr<IR::Operations::ConstIntOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerDiv(const std::shared_ptr<IR::Operations::DivOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerFunction(const std::shared_ptr<IR::Operations::FunctionOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerIf(const std::shared_ptr<IR::Operations::IfOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerMod(const std::shared_ptr<IR::Operations::ModOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerMul(const std::shared_ptr<IR::Operations::MulOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerNegate(const std::shared_ptr<IR::Operations::NegateOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerLoad(const std::shared_ptr<IR::Operations::LoadOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerLoop(const std::shared_ptr<IR::Operations::LoopOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerOr(const std::shared_ptr<IR::Operations::OrOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerProxyCall(const std::shared_ptr<IR::Operations::ProxyCallOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerReturn(const std::shared_ptr<IR::Operations::ReturnOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerStore(const std::shared_ptr<IR::Operations::StoreOperation>& operation, RegisterFrame& frame) = 0;

    virtual std::unique_ptr<CodeGenerator> lowerSub(const std::shared_ptr<IR::Operations::SubOperation>& operation, RegisterFrame& frame) = 0;
};

} // namespace NES::Nautilus::Backends

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_IRLOWERINGINTERFACE_HPP_
