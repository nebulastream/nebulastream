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

add_executable(compiler-test "UnitTests/Compiler/JITCompilerTest.cpp")
target_link_libraries(compiler-test nes-tests-util ${GTEST_LIBRARIES} nes-compiler)
add_test(NAME compiler-test COMMAND compiler-test)

add_executable(compiler-sharedlibrary-test "UnitTests/Compiler/SharedLibraryTest.cpp")
target_link_libraries(compiler-sharedlibrary-test nes-tests-util ${GTEST_LIBRARIES} nes-compiler)
add_test(NAME compiler-sharedlibrary-test COMMAND compiler-sharedlibrary-test)

if(NES_USE_MLIR)
    get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS) # MLIR_CONVERSION_LIBS are also defined
    add_executable(compiler-mlir-simpleModuleFromString-test "UnitTests/Experimental/MLIRModuleFromSimpleString.cpp")
    target_link_libraries(nes-compiler
            PUBLIC
            ${GTEST_LIBRARIES}
#            ${dialect_libs}
            MLIRExecutionEngine

#            MLIRAffineAnalysis
#            MLIRAffine
#            MLIRAffineTransforms
#            MLIRAffineUtils
#            MLIRArithmetic
#            MLIRArithmeticTransforms
#            MLIRArmNeon
#            MLIRArmSVE
#            MLIRArmSVETransforms
#            MLIRAsync
#            MLIRAsyncTransforms
#            MLIRAMX
#            MLIRAMXTransforms
#            MLIRBufferization
#            MLIRBufferizationTransforms
#            MLIRComplex
#            MLIRDLTI
            MLIREmitC
            MLIRGPUOps
            MLIRGPUTransforms
            MLIRLinalgAnalysis
            MLIRAffineBufferizableOpInterfaceImpl
            MLIRLinalgBufferizableOpInterfaceImpl
            MLIRModuleBufferization
            MLIRLinalg
            MLIRLinalgTransforms
            MLIRLinalgUtils
            MLIRLLVMIRTransforms
            MLIRLLVMIR
            MLIRNVVMIR
            MLIRROCDLIR
            MLIRMath
            MLIRMathTransforms
            MLIRMemRef
            MLIRMemRefTransforms
            MLIRMemRefUtils
            MLIROpenACC
            MLIROpenMP
            MLIRPDL
            MLIRPDLInterp
            MLIRQuant
            MLIRSCF
            MLIRSCFTransforms
            MLIRSCFUtils
            MLIRShape
            MLIRShapeOpsTransforms
            MLIRSparseTensor
            MLIRSparseTensorTransforms
            MLIRSparseTensorPipelines
            MLIRSparseTensorUtils

            MLIRSPIRV
            MLIRTensor
            MLIRTensorInferTypeOpInterfaceImpl
            MLIRTensorTilingInterfaceImpl
            MLIRTosa
            )
    target_link_libraries(compiler-mlir-simpleModuleFromString-test
            PRIVATE
            nes-compiler)
    add_test(NAME compiler-mlir-simpleModuleFromString-test COMMAND compiler-mlir-simpleModuleFromString-test)
endif(NES_USE_MLIR)

#add_executable(compiler-mlir-buffer "UnitTests/Experimental/nesTupleBuffer.cpp")
#target_link_libraries(compiler-mlir-buffer-test ${GTEST_LIBRARIES} nes-compiler)
#add_test(NAME compiler-mlir-buffer COMMAND compiler-mlir-buffer)


set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)

