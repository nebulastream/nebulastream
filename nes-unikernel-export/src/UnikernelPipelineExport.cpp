//
// Created by ls on 15.09.23.
//

#include "../include/UnikernelPipelineExport.h"
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Identifiers.hpp>
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <algorithm>
#include <any>
#include <iostream>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/GlobalVariable.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Metadata.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/InitLLVM.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/Transforms/Utils/ModuleUtils.h>
#include <mlir/Conversion/ArithToLLVM/ArithToLLVM.h>
#include <mlir/Conversion/ControlFlowToLLVM/ControlFlowToLLVM.h>
#include <mlir/Conversion/FuncToLLVM/ConvertFuncToLLVM.h>
#include <mlir/Conversion/FuncToLLVM/ConvertFuncToLLVMPass.h>
#include <mlir/Conversion/LLVMCommon/TypeConverter.h>
#include <mlir/Dialect/Func/IR/FuncOps.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/Parser/Parser.h>
#include <mlir/Pass/Pass.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Support/LLVM.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMIRToLLVMTranslation.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <mlir/Target/LLVMIR/ModuleTranslation.h>
#include <ranges>
#include <utility>

#define UNIKERNEL_FUNCTION_TAG 723468
namespace stdv = std::ranges::views;
namespace stdr = std::ranges;

struct LLVMLoweringPass : public mlir::PassWrapper<LLVMLoweringPass, mlir::OperationPass<mlir::ModuleOp>> {
    MLIR_DEFINE_EXPLICIT_INTERNAL_INLINE_TYPE_ID(LLVMLoweringPass)

    void getDependentDialects(mlir::DialectRegistry& registry) const override { registry.insert<mlir::LLVM::LLVMDialect>(); }

    void runOnOperation() final;
};

void LLVMLoweringPass::runOnOperation() {

    mlir::ConversionTarget target(getContext());
    target.addLegalOp<mlir::ModuleOp>();
    target.addLegalDialect<mlir::LLVM::LLVMDialect>();

    mlir::LLVMTypeConverter typeConverter(&getContext());

    mlir::RewritePatternSet patterns(&getContext());
    mlir::arith::populateArithToLLVMConversionPatterns(typeConverter, patterns);
    mlir::cf::populateControlFlowToLLVMConversionPatterns(typeConverter, patterns);
    mlir::populateFuncToLLVMConversionPatterns(typeConverter, patterns);

    auto module = getOperation();
    auto result = applyFullConversion(module, target, std::move(patterns));
    if (failed(result))
        signalPassFailure();
}

/// Create a pass for lowering operations the remaining `Toy` operations, as
/// well as `Affine` and `Std`, to the LLVM dialect for codegen.
std::unique_ptr<mlir::Pass> createLowerToLLVMPass() { return std::make_unique<LLVMLoweringPass>(); }

std::string mangledStageFunctionName(llvm::StringRef functionName, uint64_t pipelineID) {
    if (functionName == "execute") {
        return "_ZN3NES9Unikernel5StageILm" + std::to_string(pipelineID)
            + "EE7executeERNS0_33UnikernelPipelineExecutionContextEPNS_7Runtime13WorkerContextERNS5_11TupleBufferE";
    } else {
        return "_ZN3NES9Unikernel5StageILm" + std::to_string(pipelineID) + "EE" + std::to_string(functionName.size())
            + functionName.str() + "ERNS0_33UnikernelPipelineExecutionContextEPNS_7Runtime13WorkerContextE";
    }
}

std::unique_ptr<llvm::Module> convertToLLVM(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir, llvm::LLVMContext& context) {
    mlir::MLIRContext mlirContext;
    mlir::registerLLVMDialectTranslation(mlirContext);
    mlirContext.loadDialect<mlir::LLVM::LLVMDialect>();
    mlirContext.loadDialect<mlir::func::FuncDialect>();
    NES::Nautilus::Backends::MLIR::MLIRLoweringProvider loweringProvider(mlirContext);
    auto mlirModule = loweringProvider.generateModuleFromIR(std::move(ir));
    auto passManager = mlir::PassManager(mlirModule.get()->getContext());
    passManager.addPass(createLowerToLLVMPass());

    if (failed(passManager.run(*mlirModule))) {
        llvm::errs() << "Failed to convert to LLVM IR\n";
    }

    return mlir::translateModuleToLLVMIR(*mlirModule, context);
}

void importFunction(llvm::Module& module, llvm::StringRef name, llvm::Module& llvmModule) {
    auto* mapi = new llvm::ValueToValueMapTy();
    auto& mapper = *mapi;
    for (auto& fn : llvmModule.functions()) {
        if (!fn.isDeclaration())
            continue;

        if (fn.getName() == "malloc" || fn.getName() == "free")
            continue;
        auto copiedDef = module.getOrInsertFunction(fn.getName(), fn.getFunctionType());
        mapper[&fn] = copiedDef.getCallee();
    }

    auto old_fn = llvmModule.getFunction("execute");
    auto exec_fn_callee = module.getOrInsertFunction(name, old_fn->getFunctionType());
    auto exec_fn = llvm::dyn_cast<llvm::Function>(exec_fn_callee.getCallee());
    exec_fn->addMetadata(UNIKERNEL_FUNCTION_TAG, *llvm::dyn_cast<llvm::MDNode>(llvm::MDString::get(module.getContext(), "yes")));
    exec_fn->deleteBody();

    mapper[old_fn] = exec_fn;

    for (const auto& fnArg : old_fn->args()) {
        mapper[&fnArg] = exec_fn->getArg(fnArg.getArgNo());
    }

    llvm::SmallVector<llvm::ReturnInst*, 8> returns;
    llvm::CloneFunctionInto(exec_fn, old_fn, mapper, llvm::CloneFunctionChangeType::DifferentModule, returns);
}

void NES::Unikernel::UnikernelPipelineExport::exportPipelineIntoModule(
    llvm::Module& module,
    PipelineId pipelineId,
    NES::Runtime::Execution::CompiledExecutablePipelineStage* stage) {
    using namespace llvm;
    using namespace NES::Nautilus::Backends::MLIR;

    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto dumpHelper = NES::DumpHelper::create("UnikernelExport", false, false);

    auto llvmModule = convertToLLVM(stage->createIR(dumpHelper, timer), module.getContext());
    importFunction(module, mangledStageFunctionName("execute", pipelineId), *llvmModule);
    auto llvmSetupModule = convertToLLVM(stage->setupIR(), module.getContext());
    importFunction(module, mangledStageFunctionName("setup", pipelineId), *llvmSetupModule);
    auto llvmTerminateModule = convertToLLVM(stage->closeIR(), module.getContext());
    importFunction(module, mangledStageFunctionName("terminate", pipelineId), *llvmTerminateModule);
}
