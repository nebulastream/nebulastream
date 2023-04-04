
FlounderLoweringProvider::FlounderFrame
FlounderLoweringProvider::LoweringContext::processBlockInvocation(IR::Operations::BasicBlockInvocation& bi,
                                                                  FlounderFrame& parentFrame) {

    FlounderFrame blockFrame;
    auto blockInputArguments = bi.getArguments();
    auto blockTargetArguments = bi.getBlock()->getArguments();
    for (uint64_t i = 0; i < blockInputArguments.size(); i++) {
        auto blockArgument = blockInputArguments[i]->getIdentifier();
        auto blockTargetArgument = blockTargetArguments[i]->getIdentifier();
        auto parentFrameFlounderValue = parentFrame.getValue(blockArgument);
        // if the value is no a vrec, we have to materialize it before invoicing the subframe.
        if (parentFrameFlounderValue->type() != flounder::VREG) {
            auto argumentVreg = program.vreg(blockArgument.data());
            program << program.request_vreg64(argumentVreg);
            program << program.mov(argumentVreg, parentFrameFlounderValue);
            parentFrameFlounderValue = argumentVreg;
        }

        if (parentFrame.contains(blockTargetArgument.data())) {
            auto targetFrameFlounderValue = parentFrame.getValue(blockTargetArgument.data());
            program << program.mov(targetFrameFlounderValue, parentFrameFlounderValue);
            blockFrame.setValue(blockTargetArgument.data(), parentFrameFlounderValue);
        } else {
            auto targetVreg = program.vreg(blockTargetArgument.data());
            program << program.request_vreg64(targetVreg);
            program << program.mov(targetVreg, parentFrameFlounderValue);
            blockFrame.setValue(blockTargetArgument.data(), targetVreg);
        }
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
            // program << program.clear(vregNode);
        }
    }
    return blockFrame;
}
void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AddOperation> addOpt,
                                                        FlounderFrame& frame) {

    auto leftInput = addOpt->getLeftInput();
    flounder::Node* leftFlounderRef = frame.getValue(leftInput->getIdentifier());
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());

    if (leftInput->getStamp()->isAddress() && addOpt->getUsages().size() == 1
        && addOpt->getUsages()[0]->getOperationType() == IR::Operations::Operation::LoadOp) {
        auto result = program.mem_add(leftFlounderRef, rightInput);
        frame.setValue(addOpt->getIdentifier(), result);
        return;
    }

    if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
        // the operation has shared access to the left input -> create new result register
        auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
        program << program.mov(result, leftFlounderRef);
        leftFlounderRef = result;
    }

    // perform add
    auto addFOp = program.add(leftFlounderRef, rightInput);
    frame.setValue(addOpt->getIdentifier(), leftFlounderRef);
    program << addFOp;

    // clear registers if we dont used them
    if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
        //program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    }
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::MulOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto leftInput = addOpt->getLeftInput();
    flounder::Node* leftFlounderRef = frame.getValue(leftInput->getIdentifier());

    if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
        // the operation has shared access to the left input -> create new result register
        auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
        program << program.mov(result, leftFlounderRef);
        leftFlounderRef = result;
    }
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());

    auto addFOp = program.imul(leftFlounderRef, rightInput);
    frame.setValue(addOpt->getIdentifier(), leftFlounderRef);
    program << addFOp;
    // clear registers if we dont used them
    if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
        // program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    }
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::SubOperation> addOpt,
                                                        FlounderFrame& frame) {
    auto leftInput = addOpt->getLeftInput();
    flounder::Node* leftFlounderRef = frame.getValue(leftInput->getIdentifier());

    if (leftInput->getUsages().size() > 1 || leftFlounderRef->type() != flounder::VREG) {
        // the operation has shared access to the left input -> create new result register
        auto result = createVreg(addOpt->getIdentifier(), addOpt->getStamp(), frame);
        program << program.mov(result, leftFlounderRef);
        leftFlounderRef = result;
    }
    auto rightInput = frame.getValue(addOpt->getRightInput()->getIdentifier());

    auto addFOp = program.sub(leftFlounderRef, rightInput);
    frame.setValue(addOpt->getIdentifier(), leftFlounderRef);
    program << addFOp;
    // clear registers if we dont used them
    if (addOpt->getRightInput()->getUsages().size() == 1 && rightInput->type() == flounder::VREG) {
        // program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(rightInput));
    }
    return;
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::CompareOperation>, FlounderFrame&) {
    /*auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(opt);

    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    if (leftInput->type() != flounder::VREG) {
        auto tempVreg = createVreg(compOpt->getIdentifier(), compOpt->getStamp(), frame);
        program << program.mov(tempVreg, leftInput);
        program << program.cmp(tempVreg, rightInput);
        program << program.clear(tempVreg);
    } else {
        program << program.cmp(leftInput, rightInput);
    }*/
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

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::AndOperation>, FlounderFrame&) {
    /*auto resultVreg = createVreg(opt->getIdentifier(), opt->getStamp(), frame);
    auto leftInput = frame.getValue(opt->getLeftInput()->getIdentifier());
    program << program.mov(resultVreg, leftInput);
    auto rightInput = frame.getValue(opt->getRightInput()->getIdentifier());
    // cmp
    program << program.and_(resultVreg, rightInput);
    frame.setValue(opt->getIdentifier(), resultVreg);*/
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

void FlounderLoweringProvider::LoweringContext::processAnd(std::shared_ptr<IR::Operations::AndOperation> andOpt,
                                                           FlounderFrame& frame,
                                                           flounder::LabelNode* falseCase) {
    if (andOpt->getLeftInput()->getOperationType() == IR::Operations::Operation::CompareOp) {
        auto left = std::static_pointer_cast<IR::Operations::CompareOperation>(andOpt->getLeftInput());
        processCmp(left, frame, falseCase);
    } else if (andOpt->getLeftInput()->getOperationType() == IR::Operations::Operation::AndOp) {
        auto left = std::static_pointer_cast<IR::Operations::AndOperation>(andOpt->getLeftInput());
        processAnd(left, frame, falseCase);
    } else if (andOpt->getLeftInput()->getOperationType() == IR::Operations::Operation::ConstBooleanOp) {

    } else {
        NES_THROW_RUNTIME_ERROR("Left is not a compare operation but a " << andOpt->getLeftInput()->toString());
    }

    if (andOpt->getRightInput()->getOperationType() == IR::Operations::Operation::CompareOp) {
        auto right = std::static_pointer_cast<IR::Operations::CompareOperation>(andOpt->getRightInput());
        processCmp(right, frame, falseCase);
    } else if (andOpt->getRightInput()->getOperationType() == IR::Operations::Operation::AndOp) {
        auto right = std::static_pointer_cast<IR::Operations::AndOperation>(andOpt->getRightInput());
        processAnd(right, frame, falseCase);
    } else {
        NES_THROW_RUNTIME_ERROR("Right is not a compare operation but a " << andOpt->getRightInput()->toString());
    }
}

void FlounderLoweringProvider::LoweringContext::processCmp(std::shared_ptr<IR::Operations::CompareOperation> compOpt,
                                                           FlounderFrame& frame,
                                                           flounder::LabelNode* falseCase) {
    auto leftInput = frame.getValue(compOpt->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(compOpt->getRightInput()->getIdentifier());
    if (leftInput->type() != flounder::VREG) {
        auto tempVreg = createVreg(compOpt->getIdentifier(), compOpt->getStamp(), frame);
        program << program.mov(tempVreg, leftInput);
        program << program.cmp(tempVreg, rightInput);
        program << program.clear(tempVreg);
    } else {
        program << program.cmp(leftInput, rightInput);
    }

    switch (compOpt->getComparator()) {
        case IR::Operations::CompareOperation::IEQ: program << program.jne(falseCase); break;
        case IR::Operations::CompareOperation::ISLT: program << program.jge(falseCase); break;
        case IR::Operations::CompareOperation::INE: program << program.je(falseCase); break;
        default: NES_THROW_RUNTIME_ERROR("No handler for comp");
    }
}

void FlounderLoweringProvider::LoweringContext::process(std::shared_ptr<IR::Operations::IfOperation> ifOpt,
                                                        FlounderFrame& frame) {
    auto trueLabel = program.label("Block_" + ifOpt->getTrueBlockInvocation().getBlock()->getIdentifier());
    auto falseLabel = program.label("Block_" + ifOpt->getFalseBlockInvocation().getBlock()->getIdentifier());
    // clear all non args
    auto falseBlockFrame = processBlockInvocation(ifOpt->getFalseBlockInvocation(), frame);
    auto trueBlockFrame = processBlockInvocation(ifOpt->getTrueBlockInvocation(), frame);

    auto booleanValue = ifOpt->getValue();
    if (booleanValue->getOperationType() == IR::Operations::Operation::OrOp) {
        program << program.jnz(trueLabel);
    } else if (booleanValue->getOperationType() == IR::Operations::Operation::AndOp) {
        auto comp = std::static_pointer_cast<IR::Operations::AndOperation>(ifOpt->getValue());
        processAnd(comp, frame, falseLabel);
        program << program.jmp(trueLabel);
    } else if (booleanValue->getOperationType() == IR::Operations::Operation::NegateOp) {
        auto negateOperation = std::static_pointer_cast<IR::Operations::NegateOperation>(ifOpt->getValue());
        auto comp = std::static_pointer_cast<IR::Operations::CompareOperation>(negateOperation->getInput());
        processCmp(comp, frame, trueLabel);
        program << program.jmp(falseLabel);
    } else {
        auto comp = std::static_pointer_cast<IR::Operations::CompareOperation>(ifOpt->getValue());
        processCmp(comp, frame, falseLabel);
        program << program.jmp(trueLabel);
    }

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
        case IR::Operations::Operation::ConstBooleanOp: {
            auto constInt = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(opt);
            auto flounderConst = program.constant64(constInt->getValue() ? 1 : 0);
            frame.setValue(constInt->getIdentifier(), flounderConst);
            return;
        }
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
        case IR::Operations::Operation::SubOp: {
            auto subOpt = std::static_pointer_cast<IR::Operations::SubOperation>(opt);
            process(subOpt, frame);
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
                if (returnFOp->type() == flounder::VREG) {
                    //program << program.clear(static_cast<flounder::VirtualRegisterIdentifierNode*>(returnFOp));
                }
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
        }
        case IR::Operations::Operation::AndOp: {
            auto call = std::static_pointer_cast<IR::Operations::AndOperation>(opt);
            process(call, frame);
            return;
        }
        default: {
            //  NES_THROW_RUNTIME_ERROR("Operation " << opt->toString() << " not handled");
            return;
        }
    }
}

}// namespace NES::ExecutionEngine::Experimental::Flounder