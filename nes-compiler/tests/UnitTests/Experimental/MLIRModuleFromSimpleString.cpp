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

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <string>

#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/AsmState.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/InitAllDialects.h>
#include <mlir/Parser.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <mlir/Transforms/DialectConversion.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Conversion/SCFToStandard/SCFToStandard.h>
#include <llvm/ADT/StringRef.h>


namespace NES::Compiler {
class MLIRModuleFromSimpleString : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRBufferTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("MLIRBufferTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("MLIRBufferTest test class TearDownTestCase."); }
};


TEST(MLIRModuleFromSimpleString, executeSimpleStringMLIR) {
    mlir::registerAsmPrinterCLOptions();
    mlir::registerMLIRContextCLOptions();
    mlir::registerPassManagerCLOptions();

    std::string moduleStr = R"mlir(
        llvm.func @printf(!llvm.ptr<i8>, ...) -> i32
        llvm.mlir.global internal constant @nl("\0A\00")
        llvm.mlir.global external @TB() : !llvm.array<2 x i32>
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

    const std::string mlirFilepath = "../../../nes-compiler/src/Experimental/generatedMLIR/simpleString.mlir";
    MLIRUtility::DebugFlags debugFlags{};
    debugFlags.comments = true;
    debugFlags.enableDebugInfo = false;
    debugFlags.prettyDebug = false;
    auto mlirUtility = std::make_shared<MLIRUtility>(mlirFilepath, false);
    if (mlirUtility->loadModuleFromString(moduleStr, &debugFlags)) {
        assert(false);
    }

    int testIntArray[2]{37,38};
    const std::vector<std::string> symbolNames{"TB"};
    // Order must match symbolNames order!
    const std::vector<llvm::JITTargetAddress> jitAddresses{
        llvm::pointerToJITTargetAddress(&testIntArray)};
    mlirUtility->runJit(symbolNames, jitAddresses);

    assert(true);
}


}// namespace NES::Compiler