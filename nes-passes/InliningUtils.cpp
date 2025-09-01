#include "InliningUtils.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/Transforms/Utils/ModuleUtils.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/DebugInfo.h"

void cloneDependencyDeclarations (Function &inlineFunction, Module &wrapperModule, ValueToValueMapTy &v2vMap, std::vector<SymbolInfo> &symbols) {
	// iterate over all global variables used by the inline function
	for (auto &BB : inlineFunction) {
		for (auto &I : BB) {
			for (Value *V : I.operands()) {
				if (auto *GV = dyn_cast<GlobalValue>(V)) {
					if (GV == &inlineFunction) continue;
					if (v2vMap.find(GV) != v2vMap.end()) { // if not yet seen
						if (auto *dependencyFunction = dyn_cast<Function>(GV)) {
							if (!dependencyFunction->isIntrinsic()) {
								// clone function declaration to the wrapper module
								auto *decl = Function::Create(
									dependencyFunction->getFunctionType(),
									GlobalValue::ExternalLinkage,
									dependencyFunction->getAddressSpace(),
									inlineFunction.getName() + "." + dependencyFunction->getName(),
									&wrapperModule
								);
								v2vMap[dependencyFunction] = decl;
								symbols.push_back({decl->getName().str(), const_cast<GlobalValue *>(GV)});
							} else { // intrinsics require special handling to avoid some LLVM errors
							    auto *existingIntrinsic = wrapperModule.getFunction(dependencyFunction->getName());
							    if (!existingIntrinsic)
							    {
							        auto *decl = Function::Create(dependencyFunction->getFunctionType(),
                                        dependencyFunction->getLinkage(),
                                        dependencyFunction->getAddressSpace(),
                                        dependencyFunction->getName(),
                                        &wrapperModule);
							        decl->copyAttributesFrom(dependencyFunction);
							        v2vMap[dependencyFunction] = decl;
							    } else
							    {
							        v2vMap[dependencyFunction] = existingIntrinsic;
							    }

							}
						} else if (const auto *dependencyVariable = dyn_cast<GlobalVariable>(GV)) {
							// also clone global constants, which is required for e.g. some exception handling
							if (dependencyVariable->hasInitializer() && dependencyVariable->isConstant()) {
								auto *init = const_cast<Constant *>(dependencyVariable->getInitializer());

								auto *newGV = new GlobalVariable(
									wrapperModule,
									dependencyVariable->getValueType(),
									true,
									dependencyVariable->getLinkage(),
									init,
									dependencyVariable->getName(),
									nullptr,
									dependencyVariable->getThreadLocalMode(),
									dependencyVariable->getAddressSpace()
								);
								newGV->setUnnamedAddr(dependencyVariable->getUnnamedAddr());
								newGV->setAlignment(dependencyVariable->getAlign());
								v2vMap[dependencyVariable] = newGV;
							}
						} else {
							// TODO: may add handling for other global value types, e.g. non const global variables
						}
					}
				}
			}
		}
	}
}

std::tuple<std::string, std::vector<SymbolInfo>> serializeFunctionWithDependencySymbols(Function &inlineFunction) {
	std::vector<SymbolInfo> symbols;

	// Create module holding the extracted function and its dependencies
	Module wrapperModule = Module("func_module", inlineFunction.getContext());

	// Clone inline function to the wrapper module
	Function *clonedFunc = Function::Create(inlineFunction.getFunctionType(), inlineFunction.getLinkage(), inlineFunction.getName(), &wrapperModule);
	ValueToValueMapTy v2vMap;
	for (auto originalIt = inlineFunction.arg_begin(), cloneIt = clonedFunc->arg_begin(); originalIt != inlineFunction.arg_end(); ++originalIt, ++cloneIt)
		v2vMap[&*originalIt] = &*cloneIt;
	SmallVector<ReturnInst *, 8> returnInst;
	CloneFunctionInto(clonedFunc, &inlineFunction, v2vMap, CloneFunctionChangeType::DifferentModule, returnInst);
	clonedFunc->addFnAttr("is_target");
    clonedFunc->setLinkage(GlobalValue::ExternalLinkage),

	// Clone declarations of all dependencies and store in symbol vector
	cloneDependencyDeclarations(inlineFunction, wrapperModule, v2vMap, symbols);

	// clone personality function of the inline function if it exists (otherwise some functions cant be properly inlined)
	if (inlineFunction.hasPersonalityFn()) {
		Constant *srcPersonalityConst = inlineFunction.getPersonalityFn();
		if (auto *srcPersonality = dyn_cast<Function>(srcPersonalityConst)) {

			auto *clonedPersonality = Function::Create(srcPersonality->getFunctionType(), srcPersonality->getLinkage(), srcPersonality->getName(), &wrapperModule);
			for (auto originalIt = srcPersonality->arg_begin(), cloneIt = clonedFunc->arg_begin(); originalIt != srcPersonality->arg_end(); ++originalIt, ++cloneIt)
				v2vMap[&*originalIt] = &*cloneIt;

			SmallVector<ReturnInst *, 8> personalityReturnInst;
			CloneFunctionInto(clonedPersonality, srcPersonality, v2vMap, CloneFunctionChangeType::LocalChangesOnly, personalityReturnInst);

			clonedFunc->setPersonalityFn(clonedPersonality);
			v2vMap[srcPersonality] = clonedPersonality;
		}
	}
	RemapFunction(*clonedFunc, v2vMap, RF_NoModuleLevelChanges);

	StripDebugInfo(wrapperModule); // to suppress some llvm warning messages for invalid debug info

	// Verify integrity of the new module
    // will automatically print the precise reasons if the verification fails, which is key for debugging functions that cant be inlined yet)
	if (verifyModule(wrapperModule, &errs())) {
		errs() << "LLVM module verification failed.\n";
		report_fatal_error("Verification of cloned inline function failed. Possibly due to use of global variables, aliases, inline asm, etc. Tried to inline function: " + inlineFunction.getName());
	}

	// serialize wrapperModule to bitcode string
	SmallVector<char, 0> buffer;
	raw_svector_ostream OS(buffer);
	WriteBitcodeToFile(wrapperModule, OS);
	std::string bitcodeStr(buffer.begin(), buffer.end());
	return std::make_tuple(std::move(bitcodeStr), std::move(symbols));
}

void insertBitcodeRegistryCall(std::shared_ptr<IRBuilder<>> builder, Function *bitcodeRegistrationFunction, Function &targetFunction, std::string &bitcodeStr) {
	auto &ctx = builder->getContext();
	ArrayRef<uint8_t> bitcode((const uint8_t *)bitcodeStr.data(), bitcodeStr.size());

	// Create types
	auto *int8Ty = IntegerType::get(ctx, 8);
	auto *int8PtrTy = PointerType::get(int8Ty, 0);

	// Create LLVM constant that holds the bitcode string
	auto *bitcodeConstant = ConstantDataArray::get(ctx, bitcode);
	auto *bitcodeGV = new GlobalVariable(
		*targetFunction.getParent(), bitcodeConstant->getType(), true, GlobalValue::PrivateLinkage,
		bitcodeConstant, targetFunction.getName() + ".bitcode");
	bitcodeGV->setUnnamedAddr(GlobalValue::UnnamedAddr::Global);
	auto *bitcodePtr = builder->CreateBitCast(builder->CreateConstGEP2_32(bitcodeConstant->getType(), bitcodeGV, 0, 0), int8PtrTy);
	auto *bitcodeLen = ConstantInt::get(Type::getInt64Ty(ctx), bitcode.size());

	// Insert call to registration function
	auto *funcPtr = ConstantExpr::getBitCast(&targetFunction, int8PtrTy);
	builder->CreateCall(bitcodeRegistrationFunction, {funcPtr, bitcodePtr, bitcodeLen});
}

void insertSymbolRegistryCalls(std::shared_ptr<IRBuilder<>> builder, Function *symbolRegistrationFunction, std::vector<SymbolInfo> &symbols) {
	LLVMContext &ctx = builder->getContext();

	// Create types
	auto *int8Ty = IntegerType::get(ctx, 8);
	auto *int8PtrTy = PointerType::get(int8Ty, 0);

	for (auto &symbol : symbols) {
		ArrayRef<uint8_t> symbolName((const uint8_t *)symbol.name.data(), symbol.name.size());

		// Create LLVM values holding the symbol names and pointers
		auto *symbolNameConstant = ConstantDataArray::get(ctx, symbolName);
		auto *symbolNameGV = new GlobalVariable(
			*symbolRegistrationFunction->getParent(), symbolNameConstant->getType(), true, GlobalValue::PrivateLinkage,
			symbolNameConstant, symbol.name);
		symbolNameGV->setUnnamedAddr(GlobalValue::UnnamedAddr::Global);
		auto *symbolNamePtr = builder->CreateBitCast(builder->CreateConstGEP2_32(symbolNameConstant->getType(), symbolNameGV, 0, 0), int8PtrTy);
		auto *symbolNameLen = ConstantInt::get(Type::getInt64Ty(ctx), symbol.name.size());

		// Insert call to registration function
		auto *symbolPtr = builder->CreateBitCast(symbol.ptr, int8PtrTy);
		builder->CreateCall(symbolRegistrationFunction, {symbolNamePtr, symbolNameLen, symbolPtr});
	}
}
