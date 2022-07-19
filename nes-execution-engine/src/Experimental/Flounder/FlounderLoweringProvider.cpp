#include <Experimental/Flounder/FlounderLoweringProvider.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/AndOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/OrOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <flounder/compiler.h>
#include <flounder/executable.h>
#include <flounder/statement.h>

namespace NES::ExecutionEngine::Experimental::Flounder {

FlounderLoweringProvider::FlounderLoweringProvider(){};

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::lower(std::shared_ptr<IR::NESIR> ir) {
    auto ctx = LoweringContext(ir);
    return ctx.process(compiler);
}

FlounderLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::NESIR> ir) : ir(ir) {}

std::unique_ptr<flounder::Executable> FlounderLoweringProvider::LoweringContext::process(flounder::Compiler& compiler) {

    auto root = ir->getRootOperation();
    this->process(root);

    auto executable = std::make_unique<flounder::Executable>();

    NES_INFO(program.to_string());
    const auto is_compilation_successful = compiler.compile(program, *executable.get());
    NES_INFO(executable->code().value());
    return executable;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::FunctionOperation> functionOperation) {
    FlounderFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    for (auto i = 0ul; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto* arg = program.vreg(argument->getIdentifier());
        rootFrame.setValue(argument->getIdentifier(), arg);
        program << program.request_vreg64(arg);
        program << program.get_argument(i, arg);
    }

    this->process(functionBasicBlock, rootFrame);

    auto blockLabel = program.label("Block_return");
    program << program.section(blockLabel);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::BasicBlock> block, FlounderFrame& parentFrame) {

    if (!this->activeBlocks.contains(block->getIdentifier())) {
        this->activeBlocks.emplace(block->getIdentifier());
        // FlounderFrame blockFrame;
        auto blockLabel = program.label("Block_" + block->getIdentifier());
        program << program.section(blockLabel);

        for (auto opt : block->getOperations()) {
            this->process(opt, parentFrame);
        }

        for (auto& item : parentFrame.getContent()) {
            if (item.second->type() == flounder::NodeType::VREG) {
                auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
               // program << program.clear(vregNode);
            }
        }
    }
}

void FlounderLoweringProvider::LoweringContext::processInline(std::shared_ptr<IR::BasicBlock> block, FlounderFrame& parentFrame) {

    //if (!this->activeBlocks.contains(block->getIdentifier())) {
    //   this->activeBlocks.emplace(block->getIdentifier());
    // FlounderFrame blockFrame;
    //auto blockLabel = program.label("Block_" + block->getIdentifier());
    //program << program.section(blockLabel);

    for (auto opt : block->getOperations()) {
        this->process(opt, parentFrame);
    }

    for (auto& item : parentFrame.getContent()) {
        if (item.second->type() == flounder::NodeType::VREG) {
            auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
      //      program << program.clear(vregNode);
        }
    }
    // }
}

flounder::VirtualRegisterIdentifierNode*
FlounderLoweringProvider::LoweringContext::createVreg(IR::Operations::OperationIdentifier id,
                                                      IR::Types::StampPtr,
                                                      FlounderFrame& frame) {
    auto* result = program.vreg(id.c_str());
    frame.setValue(id, result);
    program << program.request_vreg64(result);
    return result;
}

FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                  FlounderFrame& parentFrame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto targetVreg = program.vreg(blockTargetArguments[i]->getIdentifier());
        program << program.request_vreg64(targetVreg);
        program << program.mov(targetVreg, parentFrame.getValue(blockInputArguments[i]->getIdentifier()));
        blockFrame.setValue(blockTargetArguments[i]->getIdentifier(), targetVreg);
    }
    return blockFrame;
}

FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processInlineBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                        FlounderFrame& parentFrame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    auto inputArguments = std::set<std::string>();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        inputArguments.emplace(blockInputArguments[i]->getIdentifier());
        blockFrame.setValue(blockTargetArguments[i]->getIdentifier(),
                            parentFrame.getValue(blockInputArguments[i]->getIdentifier()));
    }
    for (auto& item : parentFrame.getContent()) {
        if (!inputArguments.contains(item.first) && item.second->type() == flounder::NodeType::VREG) {
            auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
            program << program.clear(vregNode);
        }
    }
    return blockFrame;
}
void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AddOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    program << program.mov(result, leftInput);
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto addFOp = program.add(result, rightInput);
    program << addFOp;
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::MulOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
    auto leftInput = frame.getValue(addOpt->getLeftInput()->getIdentifier());
    program << program.mov(result, leftInput);
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());
    auto addFOp = program.imul(result, rightInput);
    program << addFOp;
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::CompareOperation> opt,
                                                        FlounderFrame& frame) {
    auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
    auto resultVreg = createVreg(compOpt->getIdentifier(), compOpt->getStamp(), frame);
    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
    program << program.mov(resultVreg, leftInput);
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    // cmp
    program << program.cmp(resultVreg, rightInput);
    program << program.clear(resultVreg);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::OrOperation> opt, FlounderFrame& frame) {
    auto compOpt = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
    //auto resultVreg = createVreg(compOpt->getIdentifier(), compOpt->getStamp(), frame);
    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
   // program << program.mov(resultVreg, leftInput);
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    // cmp
    program << program.or_(leftInput, rightInput);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AndOperation> opt, FlounderFrame& frame) {
    auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    auto leftInput = frame.getValue(opt->getLeftInput()->getIdentifier());
    program << program.mov(resultVreg, leftInput);
    auto rightInput = frame.getValue(opt->getRightInput()->getIdentifier());
    // cmp
    program << program.and_(resultVreg, rightInput);
    frame.setValue(opt->getIdentifier(), resultVreg);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::LoadOperation> opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto memNode = program.mem_at(address);
    auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    program << program.mov(resultVreg, memNode);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::StoreOperation> opt,
                                                        FlounderFrame& frame) {
    auto address = frame.getValue(opt->getAddress()->getIdentifier());
    auto value = frame.getValue(opt->getValue()->getIdentifier());
    auto memNode = program.mem_at(address);
    program << program.mov(memNode, value);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::BranchOperation> branchOp,
                                                        FlounderFrame& frame) {
    auto branchLabel = program.label("Block_" + branchOp->getNextBlockInvocation().getBlock()->getIdentifier());
    auto targetFrame = processInlineBlockInvocation(branchOp->getNextBlockInvocation(), frame);
    //program << program.jmp(branchLabel);
    processInline(branchOp->getNextBlockInvocation().getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::IfOperation> ifOpt,
                                                        FlounderFrame& frame) {
    auto trueLabel = program.label("Block_" + ifOpt->getTrueBlockInvocation().getBlock()->getIdentifier());
    auto falseLabel = program.label("Block_" + ifOpt->getFalseBlockInvocation().getBlock()->getIdentifier());
    // clear all non args
    auto trueBlockFrame = processBlockInvocation(ifOpt->getTrueBlockInvocation(), frame);

    auto booleanValue = ifOpt->getValue();
    if (booleanValue->getOperationType() == IR::Operations::Operation::OrOp) {
        program << program.jnz(trueLabel);
    } else if (booleanValue->getOperationType() == IR::Operations::Operation::AndOp) {
        auto comp = std::static_pointer_cast<IR::Operations::AndOperation>(ifOpt->getValue());
        auto andValue = frame.getValue(comp->getIdentifier());
        program << program.test(andValue, program.constant64(INT64_MAX));
        program << program.jnz(trueLabel);
    } else {
        auto comp = std::static_pointer_cast<IR::Operations::CompareOperation>(ifOpt->getValue());
        switch (comp->getComparator()) {
            case IR::Operations::CompareOperation::IEQ: program << program.je(trueLabel); break;
            case IR::Operations::CompareOperation::ISLT: program << program.jl(trueLabel); break;
            default: NES_THROW_RUNTIME_ERROR("No handler for comp");
        }
    }

    auto falseBlockFrame = processBlockInvocation(ifOpt->getFalseBlockInvocation(), frame);
    program << program.jmp(falseLabel);

    process(ifOpt->getTrueBlockInvocation().getBlock(), trueBlockFrame);
    /*for (auto& item : trueBlockFrame.getContent()) {
        if (item.second->type() == flounder::NodeType::VREG) {
            auto vregNode = static_cast<flounder::VirtualRegisterIdentifierNode*>(item.second);
            program << program.clear(vregNode);
        }
    }*/
    process(ifOpt->getFalseBlockInvocation().getBlock(), falseBlockFrame);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::ProxyCallOperation> opt,
                                                        FlounderFrame& frame) {

    std::vector<flounder::Node*> callArguments;
    for (auto arg : opt->getInputArguments()) {
        auto value = frame.getValue(arg->getIdentifier());
        // we first have to materialize constant in a register
        if (value->type() == flounder::CONSTANT) {
            auto* resultRegister = program.vreg(arg->getIdentifier());
            program << program.request_vreg64(resultRegister);
            program << program.mov(resultRegister, value);
            callArguments.emplace_back(resultRegister);
        } else {
            callArguments.emplace_back(value);
        }
    }

    if (!opt->getStamp()->isVoid()) {
        auto* resultRegister = program.vreg(opt->getIdentifier());
        program << program.request_vreg64(resultRegister);
        auto call = flounder::FunctionCall(program, (std::uintptr_t) opt->getFunctionPtr(), resultRegister);
        frame.setValue(opt->getIdentifier(), resultRegister);
        call.call({callArguments.begin(), callArguments.end()});
    } else {
        auto call = flounder::FunctionCall(program, (std::uintptr_t) opt->getFunctionPtr());
        call.call({callArguments.begin(), callArguments.end()});
    }
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::LoopOperation> opt,
                                                        FlounderFrame& frame) {

    auto loopHead = opt->getLoopHeadBlock();
    auto targetFrame = processInlineBlockInvocation(loopHead, frame);
    //program << program.jmp(branchLabel);
    processInline(loopHead.getBlock(), targetFrame);
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::Operation> opt, FlounderFrame& frame) {
    switch (opt->getOperationType()) {
        case IR::Operations::Operation::ConstIntOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstIntOperation>(opt);
            auto flounderConst = program.constant64(constInt->getConstantIntValue());
            frame.setValue(constInt->getIdentifier(), flounderConst);
            return;
        }
        case IR::Operations::Operation::AddOp: {
            auto addOpt = std::static_pointer_cast<IR::Operations::AddOperation>(opt);
            process(addOpt, frame);
            return;
        }
        case IR::Operations::Operation::MulOp: {
            auto mulOpt = std::static_pointer_cast<IR::Operations::MulOperation>(opt);
            process(mulOpt, frame);
            return;
        }
        case IR::Operations::Operation::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);
            process(compOpt, frame);
            return;
        }
        case IR::Operations::Operation::IfOp: {
            auto ifOpt = std::static_pointer_cast<IR::Operations::IfOperation>(opt);
            process(ifOpt, frame);
            return;
        }
        case IR::Operations::Operation::ReturnOp: {
            auto returnOpt = std::static_pointer_cast<IR::Operations::ReturnOperation>(opt);
            if (returnOpt->hasReturnValue()) {
                auto returnFOp = frame.getValue(returnOpt->getReturnValue()->getIdentifier());
                program << program.set_return(returnFOp);
            }
            auto branchLabel = program.label("Block_return");

            program << program.jmp(branchLabel);
            return;
        }
        case IR::Operations::Operation::BranchOp: {
            auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(opt);
            process(branchOp, frame);
            return;
        }
        case IR::Operations::Operation::LoopOp: {
            auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(opt);
            process(loopOp, frame);
            return;
        }
        case IR::Operations::Operation::LoadOp: {
            auto load = std::static_pointer_cast<IR::Operations::LoadOperation>(opt);
            process(load, frame);
            return;
        }
        case IR::Operations::Operation::StoreOp: {
            auto store = std::static_pointer_cast<IR::Operations::StoreOperation>(opt);
            process(store, frame);
            return;
        }
        case IR::Operations::Operation::ProxyCallOp: {
            auto call = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(opt);
            process(call, frame);
            return;
        }
        case IR::Operations::Operation::OrOp: {
            auto call = std::static_pointer_cast<IR::Operations::OrOperation>(opt);
            process(call, frame);
            return;
        }case IR::Operations::Operation::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, frame);
            return;
        }
        default: {
            return;
        }
    }
}

}// namespace NES::ExecutionEngine::Experimental::Flounder