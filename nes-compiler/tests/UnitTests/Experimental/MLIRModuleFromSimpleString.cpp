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
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include"llvm/Config/abi-breaking.h"

#include <string>

#include "mlir/ExecutionEngine/ExecutionEngine.h"
#include "mlir/IR/AsmState.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/InitAllDialects.h"
#include "mlir/Parser.h"
#include "mlir/Pass/Pass.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h"

#include "llvm/ADT/StringRef.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"


#include <mlir/Conversion/MemRefToLLVM/MemRefToLLVM.h>
#include <mlir/Conversion/ArithmeticToLLVM/ArithmeticToLLVM.h>
#include <mlir/Conversion/ReconcileUnrealizedCasts/ReconcileUnrealizedCasts.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <system_error>
#include "mlir/Transforms/DialectConversion.h"

#include "mlir/Conversion/SCFToStandard/SCFToStandard.h"

// ERROR CHECKING
#include"llvm/Config/abi-breaking.h"
#include "llvm/ADT/ilist.h"
#include "llvm/ADT/ilist_iterator.h"
// ERROR CHECKING

namespace NES::Compiler {
class MLIRModuleFromSimpleString : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRBufferTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRBufferTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRBufferTest test class TearDownTestCase."); }
};

/**
 * @brief lowers MLIR Dialect to LLVM Dialect
 * @param module: the MLIR module that contains the MLIR that needs to be lowered
 * @return LogicalResult: Optional (Expected) that marks whether lowering was successful
 * @note the returned LogicalResult MUST BE CHECKED, even if successful (e.g. via if-else)
 */
static mlir::LogicalResult lowerToLLVMDialect(mlir::ModuleOp module) {
    mlir::PassManager pm(module.getContext());
    pm.addPass(mlir::createMemRefToLLVMPass());
    pm.addPass(mlir::createLowerToCFGPass());
    pm.addNestedPass<mlir::FuncOp>(mlir::arith::createConvertArithmeticToLLVMPass());
    pm.addPass(mlir::createLowerToLLVMPass());
    // pm.addPass(mlir::registerToCppTranslation());
    pm.addPass(mlir::createReconcileUnrealizedCastsPass());
    return pm.run(module);
}

void printMLIRModule(mlir::OwningOpRef<mlir::ModuleOp> &module) {
    std::string mlirString;
    llvm::raw_string_ostream llvmStringStream(mlirString);
//    llvm::StringRef filePathRef("../../generatedMLIR/generated.mlir");
    llvm::StringRef filePathRef("generated.mlir");
    std::error_code basicError;
    llvm::raw_fd_ostream fileStream(filePathRef, basicError);
    mlir::OpPrintingFlags opPrintFlags;
    opPrintFlags.enableDebugInfo(true);
    module->print(llvmStringStream, opPrintFlags);
    printf("%s", mlirString.c_str());
    fileStream.write(mlirString.c_str(), mlirString.length());
}

TEST(MLIRModuleFromSimpleString, executeSimpleStringMLIR) {
    // Initialize LLVM targets.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    std::string moduleStr = R"mlir(
llvm.func @printf(!llvm.ptr<i8>, ...) -> i32
llvm.mlir.global internal constant @nl("\0A\00")
 llvm.mlir.global external @TB() : !llvm.array<2 x i32>
//    llvm.mlir.global external @TB(unit) : !llvm.array<2 x i32>
func @execute(%arg0: i32) -> i32 attributes {llvm.emit_c_interface} {
  // loop upper limit
  %c100 = arith.constant 100 : index
  %c0 = arith.constant 0 : index
  %c1 = arith.constant 1 : index
  %c0_i32 = arith.constant 0 : i32
  %0 = scf.for %arg1 = %c0 to %c100 step %c1 iter_args(%arg2 = %c0_i32) -> (i32) {
    %c42_i32 = arith.constant 42 : i32
    %1 = arith.cmpi slt, %arg2, %c42_i32 : i32
    %2 = scf.if %1 -> (i32) {
      %c1_i32 = arith.constant 1 : i32
      %3 = llvm.add %c1_i32, %arg2  : i32
      %4 = llvm.mlir.addressof @nl : !llvm.ptr<array<2 x i8>>
      %5 = llvm.mlir.addressof @TB : !llvm.ptr<array<2 x i32>>
      %6 = llvm.mlir.constant(0 : index) : i64
      %7 = llvm.getelementptr %4[%6, %6] : (!llvm.ptr<array<2 x i8>>, i64, i64) -> !llvm.ptr<i8>
      %8 = llvm.getelementptr %5[%6, %6] : (!llvm.ptr<array<2 x i32>>, i64, i64) -> !llvm.ptr<i32>
      %9 = llvm.call @printf(%7) : (!llvm.ptr<i8>) -> i32
      %10 = llvm.load %8 : !llvm.ptr<i32>
      scf.yield %10 : i32
    } else {
      %c0_i32_0 = arith.constant 0 : i32
      %3 = llvm.add %c0_i32_0, %arg2  : i32
      %4 = llvm.mlir.addressof @nl : !llvm.ptr<array<2 x i8>>
      %5 = llvm.mlir.constant(0 : index) : i64
      %6 = llvm.getelementptr %4[%5, %5] : (!llvm.ptr<array<2 x i8>>, i64, i64) -> !llvm.ptr<i8>
      %7 = llvm.call @printf(%6) : (!llvm.ptr<i8>) -> i32
      scf.yield %3 : i32
    }
    scf.yield %2 : i32
  }
  llvm.return %0 : i32
}
)mlir";
    mlir::DialectRegistry registry;
    registerAllDialects(registry);
    registerLLVMDialectTranslation(registry);
    mlir::MLIRContext context(registry);
//    //  mlir::OwningModuleRef module2 = mlir::parseSourceFile(
//    //    "/home/rudi/dima/mlir-approach/generatedMLIR/generated.mlir", &context);
//    //  auto ops = module2->body().getOps();
    mlir::OwningOpRef<mlir::ModuleOp> module2 = parseSourceString(moduleStr, &context);

    printMLIRModule(module2);
//
    auto logicalResult = lowerToLLVMDialect(*module2);
    if(logicalResult.failed()) {assert(false);}
    if(auto jitOrError = mlir::ExecutionEngine::create(*module2)) {
        std::unique_ptr<mlir::ExecutionEngine> jit = std::move(jitOrError.get());

        //===-------------------------------------------===//
        // int testInt = 35;
        int testIntArray[2]{37,38};
        const std::vector<std::string> symbolNames{"TB"};
        const std::vector<llvm::JITTargetAddress> jitAddresses{
            llvm::pointerToJITTargetAddress(&testIntArray)};
        auto runtimeSymbolMap =  [&](llvm::orc::MangleAndInterner interner) {
            auto symbolMap = llvm::orc::SymbolMap();
            for(auto symbolName : symbolNames) {
                symbolMap[interner(symbolName)] =
                    llvm::JITEvaluatedSymbol(jitAddresses.at(0), llvm::JITSymbolFlags::Exported);
            }
            return symbolMap;
        };
        jit->registerSymbols(runtimeSymbolMap);
        //===-------------------------------------------===//

        int result = 0;
        if(llvm::Error error = jit->invoke("execute", 42, mlir::ExecutionEngine::Result<int>(result))) {
            printf("result is: %d\n", result);
            assert(true);
        }
        printf("result is: %d\n", result);
        assert(true);
    }
    assert(true);
}


}// namespace NES::Compiler