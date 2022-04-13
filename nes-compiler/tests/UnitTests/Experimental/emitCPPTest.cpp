/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)
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

#include "mlir/ExecutionEngine/ExecutionEngine.h"
#include "mlir/ExecutionEngine/OptUtils.h"
#include "mlir/IR/AsmState.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OpImplementation.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/IR/Verifier.h"
#include "mlir/InitAllDialects.h"
#include "mlir/Parser.h"
#include "mlir/Pass/Pass.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Target/Cpp/CppEmitter.h"
#include "mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h"
#include "mlir/Target/LLVMIR/Export.h"
#include "mlir/Transforms/Passes.h"

#include "llvm/ADT/StringRef.h"
#include "llvm/IR/Module.h"
#include "llvm/InitializePasses.h"
#include "llvm/Support/CommandLine.h"
#include "llvm/Support/ErrorOr.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"


#include "mlir/Conversion/LLVMCommon/TypeConverter.h"
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVM.h>
#include <mlir/Conversion/LLVMCommon/ConversionTarget.h>
#include <mlir/Conversion/MemRefToLLVM/MemRefToLLVM.h>
#include <mlir/Conversion/ArithmeticToLLVM/ArithmeticToLLVM.h>
#include <mlir/Conversion/ReconcileUnrealizedCasts/ReconcileUnrealizedCasts.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <system_error>
#include "mlir/Transforms/DialectConversion.h"

#include "mlir/Conversion/SCFToStandard/SCFToStandard.h"

#include "mlir/Dialect/EmitC/IR/EmitC.h"



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

void printMLIRModule(mlir::OwningModuleRef &module) {
    std::string mlirString;
    llvm::raw_string_ostream llvmStringStream(mlirString);
    llvm::StringRef filePathRef("../../generatedMLIR/generated.mlir");
    std::error_code basicError;
    llvm::raw_fd_ostream fileStream(filePathRef, basicError);
    mlir::OpPrintingFlags opPrintFlags;
    opPrintFlags.enableDebugInfo(true);
    module->print(llvmStringStream, opPrintFlags);
    printf("%s", mlirString.c_str());
    fileStream.write(mlirString.c_str(), mlirString.length());
}

/**
 * @brief creates an MLIR module from a string, JIT compiles and executes it
 * @return 0 if successful, else 1
 */
int simpleJITOnlyTest() {
  // Initialize LLVM targets.
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  std::string moduleStr = R"mlir(
  func @execute(%arg0: i32) -> i32 attributes {llvm.emit_c_interface} {
    %c3_i32 = arith.constant 3 : i32
    %c0 = arith.constant 0 : index
    %c5 = arith.constant 5 : index
    %c1 = arith.constant 1 : index
    %sum = scf.for %arg1 = %c0 to %c5 step %c1 iter_args(%sum_iter = %c3_i32) -> (i32) {
      // %sum_next = llvm.add %sum_iter, %sum_iter : i32
      scf.yield %sum_iter : i32
    }
    return %c3_i32 : i32
  }
  )mlir";
  mlir::DialectRegistry registry;
  registerAllDialects(registry);
  registerLLVMDialectTranslation(registry);
  mlir::MLIRContext context(registry);
  mlir::OwningModuleRef module2 = parseSourceString(moduleStr, &context);
  
  std::string cppString;
  llvm::raw_string_ostream cppStringStream(cppString);
  auto emitter = mlir::emitc::translateToCpp(*module2, cppStringStream, true);
  if(emitter.succeeded()) {
    printf("%s", cppString.c_str());
  }

  printMLIRModule(module2);

  auto logicalResult = lowerToLLVMDialect(*module2);
  if(logicalResult.failed()) {return 1;}
  if(auto jitOrError = mlir::ExecutionEngine::create(*module2)) {
    std::unique_ptr<mlir::ExecutionEngine> jit = std::move(jitOrError.get());
    int result = 0;
    if(llvm::Error error = jit->invoke("execute", 42, mlir::ExecutionEngine::Result<int>(result))) {
      printf("result is: %d\n", result);
      return 0;
    }
    printf("result is: %d\n", result);
    return 0;
  }
  return 1;
}

int main() {
 return simpleJITOnlyTest();
}