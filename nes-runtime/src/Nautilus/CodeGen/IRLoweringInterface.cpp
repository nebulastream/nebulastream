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

#include <Nautilus/CodeGen/IRLoweringInterface.hpp>

#include <Nautilus/CodeGen/Segment.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::CodeGen {

std::unique_ptr<CodeGenerator> IRLoweringInterface::lowerOperation(const std::shared_ptr<IR::Operations::Operation>& operation, RegisterFrame& frame) {
    if (!operation) {
        return std::make_unique<Segment>();
    }
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::AddOp: {
            auto addOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
            return lowerAdd(addOp, frame);
        }
        case IR::Operations::Operation::OperationType::AddressOp: {
            auto addressOp = std::static_pointer_cast<IR::Operations::AddressOperation>(operation);
            return lowerAddress(addressOp, frame);
        }
        case IR::Operations::Operation::OperationType::AndOp: {
            auto andOp = std::static_pointer_cast<IR::Operations::AndOperation>(operation);
            return lowerAnd(andOp, frame);
        }
        case IR::Operations::Operation::OperationType::BasicBlockArgument: {
            auto argumentOp = std::static_pointer_cast<IR::Operations::BasicBlockArgument>(operation);
            return lowerBasicBlockArgument(argumentOp, frame);
        }
        case IR::Operations::Operation::OperationType::BlockInvocation: {
            auto invokeOp = std::static_pointer_cast<IR::Operations::BasicBlockInvocation>(operation);
            return lowerBasicBlockInvocation(invokeOp, frame);
        }
        case IR::Operations::Operation::OperationType::BitWiseAnd: {
            auto bitwiseOp = std::static_pointer_cast<IR::Operations::BitWiseAndOperation>(operation);
            return lowerBitwiseAnd(bitwiseOp, frame);
        }
        case IR::Operations::Operation::OperationType::BitWiseLeftShift: {
            auto bitwiseOp = std::static_pointer_cast<IR::Operations::BitWiseLeftShiftOperation>(operation);
            return lowerBitwiseLeftShift(bitwiseOp, frame);
        }
        case IR::Operations::Operation::OperationType::BitWiseOr: {
            auto bitwiseOp = std::static_pointer_cast<IR::Operations::BitWiseOrOperation>(operation);
            return lowerBitwiseOr(bitwiseOp, frame);
        }
        case IR::Operations::Operation::OperationType::BitWiseRightShift: {
            auto bitwiseOp = std::static_pointer_cast<IR::Operations::BitWiseRightShiftOperation>(operation);
            return lowerBitwiseRightShift(bitwiseOp, frame);
        }
        case IR::Operations::Operation::OperationType::BitWiseXor: {
            auto bitwiseOp = std::static_pointer_cast<IR::Operations::BitWiseXorOperation>(operation);
            return lowerBitwiseXor(bitwiseOp, frame);
        }
        case IR::Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(operation);
            return lowerBranch(branchOp, frame);
        }
        case IR::Operations::Operation::OperationType::ConstAddressOp: {
            auto constAddressOp = std::static_pointer_cast<IR::Operations::ConstAddressOperation>(operation);
            return lowerConstAddress(constAddressOp, frame);
        }
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constBooleanOp = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation);
            return lowerConstBoolean(constBooleanOp, frame);
        }
        case IR::Operations::Operation::OperationType::ConstFloatOp: {
            auto constFloatOp = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(operation);
            return lowerConstFloat(constFloatOp, frame);
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constIntOp = std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation);
            return lowerConstInt(constIntOp, frame);
        }
        case IR::Operations::Operation::OperationType::CastOp: {
            auto castOp = std::static_pointer_cast<IR::Operations::CastOperation>(operation);
            return lowerCast(castOp, frame);
        }
        case IR::Operations::Operation::OperationType::CompareOp: {
            auto compareOp = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);
            return lowerCompare(compareOp, frame);
        }
        case IR::Operations::Operation::OperationType::DivOp: {
            auto divOp = std::static_pointer_cast<IR::Operations::DivOperation>(operation);
            return lowerDiv(divOp, frame);
        }
        case IR::Operations::Operation::OperationType::ModOp: {
            auto modOp = std::static_pointer_cast<IR::Operations::ModOperation>(operation);
            return lowerMod(modOp, frame);
        }
        case IR::Operations::Operation::OperationType::FunctionOp: {
            auto fnOp = std::static_pointer_cast<IR::Operations::FunctionOperation>(operation);
            return lowerFunction(fnOp, frame);
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(operation);
            return lowerIf(ifOp, frame);
        }
        case IR::Operations::Operation::OperationType::LoopOp: {
            auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(operation);
            return lowerLoop(loopOp, frame);
        }
        case IR::Operations::Operation::OperationType::LoadOp: {
            auto loadOp = std::static_pointer_cast<IR::Operations::LoadOperation>(operation);
            return lowerLoad(loadOp, frame);
        }
        case IR::Operations::Operation::OperationType::MulOp: {
            auto mulOp = std::static_pointer_cast<IR::Operations::MulOperation>(operation);
            return lowerMul(mulOp, frame);
        }
        case IR::Operations::Operation::OperationType::NegateOp: {
            auto negateOp = std::static_pointer_cast<IR::Operations::NegateOperation>(operation);
            return lowerNegate(negateOp, frame);
        }
        case IR::Operations::Operation::OperationType::OrOp: {
            auto orOp = std::static_pointer_cast<IR::Operations::OrOperation>(operation);
            return lowerOr(orOp, frame);
        }
        case IR::Operations::Operation::OperationType::ProxyCallOp: {
            auto proxyCallOp = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation);
            return lowerProxyCall(proxyCallOp, frame);
        }
        case IR::Operations::Operation::OperationType::ReturnOp: {
            auto returnOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
            return lowerReturn(returnOp, frame);
        }
        case IR::Operations::Operation::OperationType::StoreOp: {
            auto storeOp = std::static_pointer_cast<IR::Operations::StoreOperation>(operation);
            return lowerStore(storeOp, frame);
        }
        case IR::Operations::Operation::OperationType::SubOp: {
            auto subOp = std::static_pointer_cast<IR::Operations::SubOperation>(operation);
            return lowerSub(subOp, frame);
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << operation->toString() << " not handled");
            break;
        }
    }
}

}
