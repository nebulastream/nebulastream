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

#pragma once

#include <string>
#include <vector>
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/IR/DebugInfo.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Value.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Utils/Cloning.h"

using namespace llvm;

struct SymbolInfo
{
    std::string name;
    Function* ptr;
};

static bool cloneOperand(
    Value* V,
    std::unordered_set<Value*>& clonedSet,
    Module& wrapperModule,
    ValueToValueMapTy& v2vMap,
    std::vector<SymbolInfo>& symbols,
    Function& inlineFunction);

// return false if we encountered an illegal LLVM::Constant case
static bool cloneValuesAndCollectDependencySymbols(
    Function& inlineFunction, Module& wrapperModule, ValueToValueMapTy& v2vMap, std::vector<SymbolInfo>& symbols)
{
    std::unordered_set<Value*> clonedSet;

    for (auto& BB : inlineFunction)
    {
        for (auto& I : BB)
        {
            for (Value* V : I.operands())
            {
                if (!cloneOperand(V, clonedSet, wrapperModule, v2vMap, symbols, inlineFunction))
                {
                    return false;
                }
            }
        }
    }
    return true;
}

static bool cloneGlobalValue(
    GlobalValue* GV,
    std::unordered_set<Value*>& clonedSet,
    Module& wrapperModule,
    ValueToValueMapTy& v2vMap,
    std::vector<SymbolInfo>& symbols,
    Function& inlineFunction)
{
    if (GV == &inlineFunction) // skip the target function
        return true;

    if (auto* F = dyn_cast<Function>(GV))
    {
        // Clone function declaration and add as linkable symbol to the symbol list
        if (!F->isIntrinsic())
        {
            auto* decl = Function::Create(
                F->getFunctionType(),
                GlobalValue::ExternalLinkage,
                F->getAddressSpace(),
                inlineFunction.getName() + "." + F->getName(),
                &wrapperModule);
            decl->copyAttributesFrom(F);
            if (decl->hasPersonalityFn())
                decl->setPersonalityFn(nullptr);
            v2vMap[F] = decl;

            symbols.push_back({decl->getName().str(), F});
        }
        else
        {
            // intrinsics require special care
            auto* existing = wrapperModule.getFunction(F->getName());
            if (!existing)
            {
                auto* decl = Function::Create(F->getFunctionType(), F->getLinkage(), F->getAddressSpace(), F->getName(), &wrapperModule);
                decl->copyAttributesFrom(F);
                v2vMap[F] = decl;
            }
            else
            {
                v2vMap[F] = existing;
            }
        }
        return true;
    }
    else if (auto* GVVar = dyn_cast<GlobalVariable>(GV))
    {
        // only works for constant variables so far; other variables would have to be linked at runtime which is not
        // yet supported
        if (GVVar->hasInitializer() && GVVar->isConstant())
        {
            // recursively clone initializer operands
            for (unsigned i = 0; i < GVVar->getInitializer()->getNumOperands(); ++i)
            {
                if (!cloneOperand(GVVar->getInitializer()->getOperand(i), clonedSet, wrapperModule, v2vMap, symbols, inlineFunction))
                {
                    return false;
                }
            }

            auto* init = cast<Constant>(MapValue(GVVar->getInitializer(), v2vMap));

            auto* newGV = new GlobalVariable(
                wrapperModule,
                GVVar->getValueType(),
                true,
                GVVar->getLinkage(),
                init,
                GVVar->getName(),
                nullptr,
                GVVar->getThreadLocalMode(),
                GVVar->getAddressSpace());
            newGV->setUnnamedAddr(GVVar->getUnnamedAddr());
            newGV->setAlignment(GVVar->getAlign());
            v2vMap[GVVar] = newGV;
            return true;
        }
        else
        {
            errs() << "Non-constant/external global variable detected in nautilus inlining pass. Please remove the "
                      "NAUT_INLINE tag from "
                      "function: "
                   << inlineFunction.getName() << "\n";
            return false;
        }
    }

    errs() << "Unknown LLVM::GlobalValue case detected in nautilus inlining pass. Please remove the "
              "NAUT_INLINE tag from "
              "function: "
           << inlineFunction.getName() << "\n";
    return false;
}

static bool cloneConstant(
    Constant* C,
    std::unordered_set<Value*>& clonedSet,
    Module& wrapperModule,
    ValueToValueMapTy& v2vMap,
    std::vector<SymbolInfo>& symbols,
    Function& inlineFunction)
{
    // clone operands recursively
    for (unsigned i = 0; i < C->getNumOperands(); ++i)
    {
        if (!cloneOperand(C->getOperand(i), clonedSet, wrapperModule, v2vMap, symbols, inlineFunction))
        {
            return false;
        }
    }

    SmallVector<Constant*, 4> newOperands;
    for (unsigned i = 0; i < C->getNumOperands(); ++i)
    {
        auto* oldOp = C->getOperand(i);
        auto* newOp = dyn_cast<Constant>(v2vMap.lookup(oldOp));
        newOperands.push_back(newOp);
    }

    // build new constant for selected supported cases
    Constant* newC = nullptr;
    if (auto* CS = dyn_cast<ConstantStruct>(C))
    {
        newC = ConstantStruct::get(CS->getType(), ArrayRef<Constant*>(newOperands));
    }
    else if (auto* CA = dyn_cast<ConstantArray>(C))
    {
        newC = ConstantArray::get(CA->getType(), ArrayRef<Constant*>(newOperands));
    }
    else if (dyn_cast<ConstantVector>(C))
    {
        newC = ConstantVector::get(ArrayRef<Constant*>(newOperands));
    }
    else if (auto* CDA = dyn_cast<ConstantDataArray>(C))
    {
        newC = ConstantDataArray::getRaw(CDA->getRawDataValues(), CDA->getNumElements(), CDA->getElementType());
    }
    else if (auto* CI = dyn_cast<ConstantInt>(C))
    {
        newC = ConstantInt::get(CI->getContext(), CI->getValue());
    }
    else if (auto* CFP = dyn_cast<ConstantFP>(C))
    {
        newC = ConstantFP::get(CFP->getType(), CFP->getValue());
    }
    else if (auto* CPN = dyn_cast<ConstantPointerNull>(C))
    {
        newC = ConstantPointerNull::get(CPN->getType());
    }
    else if (auto* CU = dyn_cast<UndefValue>(C))
    {
        newC = UndefValue::get(CU->getType());
    }
    else if (auto* CE = dyn_cast<ConstantExpr>(C))
    {
        newC = CE->getWithOperands(newOperands);
    }
    else
    {
        // tough luck
        errs() << *C << "\n";
        errs() << "Unknown LLVM::Constant case found in nautilus inlining pass. Please remove the NAUT_INLINE tag "
                  "from "
                  "function: "
               << inlineFunction.getName() << "\n";
        return false;
    }
    v2vMap[C] = newC;
    return true;
}

static bool cloneOperand(
    Value* V,
    std::unordered_set<Value*>& clonedSet,
    Module& wrapperModule,
    ValueToValueMapTy& v2vMap,
    std::vector<SymbolInfo>& symbols,
    Function& inlineFunction)
{
    if (!V || clonedSet.contains(V))
    {
        return true;
    }
    clonedSet.insert(V);

    if (dyn_cast<Argument>(V) || dyn_cast<BasicBlock>(V) || dyn_cast<InlineAsm>(V)) // No special handling required
    {
        return true;
    }
    else if (auto* OP = dyn_cast<Operator>(V)) // Recursively clone operands if required (e.g. for ConstExpr)
    {
        for (unsigned i = 0; i < OP->getNumOperands(); ++i)
        {
            if (!cloneOperand(OP->getOperand(i), clonedSet, wrapperModule, v2vMap, symbols, inlineFunction))
            {
                return false;
            }
        }
        return true;
    }
    else if (auto* GV = dyn_cast<GlobalValue>(V)) // handle global values (functions, variables, ...)
    {
        return cloneGlobalValue(GV, clonedSet, wrapperModule, v2vMap, symbols, inlineFunction);
    }
    else if (auto* C = dyn_cast<Constant>(V))
    {
        return cloneConstant(C, clonedSet, wrapperModule, v2vMap, symbols, inlineFunction);
    }

    errs() << *V << "\n";
    errs() << "Unknown LLVM::Value case found in nautilus inlining pass. Please remove the NAUT_INLINE tag "
              "from "
              "function: "
           << inlineFunction.getName() << "\n";
    return false;
}
