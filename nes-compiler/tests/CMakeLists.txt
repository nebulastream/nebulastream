# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)

set(LLVM_LINK_COMPONENTS
        Core
        Support
        nativecodegen
        OrcJIT
        )
#add_library(baseExecutables
#        ../../nes-compiler/src/Experimental/MLIR/NESAbstractionToMLIR.cpp
#        ../../nes-compiler/src/Experimental/MLIR/MLIRJIT.cpp
#        ../../nes-compiler/src/Experimental/MLIR/MLIRUtility.cpp
#        )
get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)

add_executable(compiler-test "UnitTests/Compiler/JITCompilerTest.cpp")
target_link_libraries(compiler-test nes-tests-util ${GTEST_LIBRARIES} nes-compiler)
add_test(NAME compiler-test COMMAND compiler-test)

add_executable(compiler-sharedlibrary-test "UnitTests/Compiler/SharedLibraryTest.cpp")
target_link_libraries(compiler-sharedlibrary-test nes-tests-util ${GTEST_LIBRARIES} nes-compiler)
add_test(NAME compiler-sharedlibrary-test COMMAND compiler-sharedlibrary-test)

add_executable(compiler-mlir-bufferTest "UnitTests/Experimental/MLIRBufferTest.cpp")
#target_link_libraries(nes-compiler ${GTEST_LIBRARIES} baseExecutables)
#target_link_libraries(compiler-mlir-bufferTest ${GTEST_LIBRARIES} nes-compiler)
#find_library(MLIRArt MLIRArithmetic HINTS /home/rudi/dima/nebulastream/cmake-build-debug/nes-dependencies-v15-x64-linux-nes/installed/x64-linux-nes/lib/)
#find_library(MLIRArithTrans MLIRArithmeticTransforms HINTS /home/rudi/dima/nebulastream/cmake-build-debug/nes-dependencies-v13-x64-linux-nes/installed/x64-linux-nes/lib/)
#find_library(MLIRArith2LLVM MLIRArithmeticToLLVM HINTS /home/rudi/dima/nebulastream/cmake-build-debug/nes-dependencies-v15-x64-linux-nes/installed/x64-linux-nes/lib/)
#find_library(Rec MLIRReconcileUnrealizedCasts HINTS /home/rudi/dima/nebulastream/cmake-build-debug/nes-dependencies-v15-x64-linux-nes/installed/x64-linux-nes/lib/)
#message(STATUS "Convs: ${MLIRArithToLLVM}")

#llvm_map_components_to_libnames(llvm_libs support core irreader orcjit)
llvm_map_components_to_libnames(llvm_libs ${LLVM_TARGETS_TO_BUILD} support core orcjit irreader codegen mc mcparser)

message(STATUS "Convs2: ${conversion_libs}")
message(STATUS "Convs3: ${dialect_libs}")
target_link_libraries(compiler-mlir-bufferTest
        PRIVATE
        nes-compiler
        ${GTEST_LIBRARIES}
        ${dialect_libs}
        MLIRReconcileUnrealizedCasts
        MLIRMemRefToLLVM
        ${llvm_libs}
#        LLVMCore
#        LLVMSupport
#        LLVMCodeGen
#        LLVMOrcJIT
#        ${conversion_libs}
#        ${MLIRArithTrans}
#        ${MLIRArith2LLVM}
#        ${Rec}
#        ${MLIRArt}
#        MLIRReconcileUnrealizedCasts
        #        MLIRAnalysis
        #        MLIRCallInterfaces
        #        MLIRCastInterfaces
        MLIRExecutionEngine
        #        MLIRIR
        #        MLIRLLVMCommonConversion
        #        MLIRLLVMIR
        #        MLIRLLVMToLLVMIRTranslation
        #        MLIRMemRef
        #        MLIRParser
        #        MLIRPass
        #        MLIRSideEffectInterfaces
        #        MLIRSupport
        #        MLIRTargetLLVMIRExport
        #        MLIRTransforms
        #        MLIRTargetCpp
        )
add_test(NAME compiler-mlir-bufferTest COMMAND compiler-mlir-bufferTest)

#add_executable(compiler-mlir-buffer "UnitTests/Experimental/nesTupleBuffer.cpp")
#target_link_libraries(compiler-mlir-buffer-test ${GTEST_LIBRARIES} nes-compiler)
#add_test(NAME compiler-mlir-buffer COMMAND compiler-mlir-buffer)


set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

