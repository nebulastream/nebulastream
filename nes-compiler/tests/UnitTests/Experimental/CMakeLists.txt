#set(LLVM_LINK_COMPONENTS
#        Core
#        Support
#        nativecodegen
#        OrcJIT
#        )
#
## was add_llvm_executable
#add_library(baseExecutables
#        ../src/MLIR/NESAbstractionToMLIR.cpp
#        ../src/MLIR/MLIRUtility.cpp
#        ../src/MLIR/MLIRJIT.cpp
#        )
#
#add_library(nesAbstractionImplementation
#        ../src/NESAbstraction/NESAbstractionTree.cpp
#        ../src/NESAbstraction/Nodes/NESAbstractionNode.cpp
#        ../src/NESAbstraction/Nodes/NESAbstractionForNode.cpp
#        ../src/NESAbstraction/Nodes/NESAbstractionIfNode.cpp
#        ../src/NESAbstraction/Nodes/NESAbstractionWriteNode.cpp
#        ../utility/createNESAbstraction.cpp
#        )
#
#add_executable(simpleAbstractionTest
#        simpleAbstractionTest.cpp
#        )
#
#add_executable(nesTupleBufferIntegration
#        nesTupleBuffer.cpp
#        )
#
#add_executable(emitCPPTest
#        emitCPPTest.cpp
#        )
#
#add_executable(executeStringMLIR
#        executeStringMLIR.cpp
#        )
#
#add_dependencies(simpleAbstractionTest baseExecutables)
#
#include_directories(include/)
#include_directories(${CMAKE_CURRENT_BINARY_DIR}/include/)
#
##Todo - is it possible to only load required libs?
#get_property(dialect_libs GLOBAL PROPERTY MLIR_DIALECT_LIBS)
#get_property(conversion_libs GLOBAL PROPERTY MLIR_CONVERSION_LIBS)
#
#target_link_libraries(baseExecutables
#        PRIVATE
#        ${dialect_libs}
#        ${conversion_libs}
#        MLIRAnalysis
#        MLIRCallInterfaces
#        MLIRCastInterfaces
#        MLIRExecutionEngine
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
#        )
#
## target_link_libraries(simpleAbstractionTest )
#target_link_libraries(simpleAbstractionTest PRIVATE
#        baseExecutables
#        nesAbstractionImplementation
#        )
#target_link_libraries(nesTupleBufferIntegration PRIVATE
#        baseExecutables
#        nesAbstractionImplementation
#        ${CMAKE_SOURCE_DIR}/../../dima/nebulastream/cmake-build-debug/nes-core/libnes.so
#        )
#target_link_libraries(emitCPPTest PRIVATE baseExecutables)
#target_link_libraries(executeStringMLIR PRIVATE baseExecutables)