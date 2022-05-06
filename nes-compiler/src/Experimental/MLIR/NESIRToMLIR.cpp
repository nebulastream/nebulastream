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

#include <Experimental/MLIR/NESIRToMLIR.hpp>



#include "mlir/Dialect/Arithmetic/IR/Arithmetic.h"
#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
#include "mlir/Dialect/LLVMIR/LLVMTypes.h"
#include "mlir/IR/Attributes.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Support/LLVM.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include <cstdint>
#include <memory>
#include <vector>

using namespace mlir;

    //==-----------------------==//
    //==-- UTILITY FUNCTIONS --==//
    //==-----------------------==//
    mlir::Type MLIRGenerator::getMLIRType(NES::Operation::BasicType type) {
        switch (type) {
            case NES::Operation::INT1:
            case NES::Operation::BOOLEAN: return builder->getIntegerType(1);
            case NES::Operation::INT8:
            case NES::Operation::CHAR: return builder->getIntegerType(8);
            case NES::Operation::INT16: return builder->getIntegerType(16);
            case NES::Operation::INT32: return builder->getIntegerType(32);
            case NES::Operation::INT64: return builder->getIntegerType(64);
            case NES::Operation::FLOAT: return Float32Type::get(context);
            case NES::Operation::DOUBLE: return Float64Type::get(context);
            default: return builder->getIntegerType(32);
        }
    }

    mlir::Value MLIRGenerator::getConstInt(Location loc, int numBits, int64_t value) {
        return builder->create<LLVM::ConstantOp>(loc,
                                                 builder->getIntegerType(numBits),
                                                 builder->getIntegerAttr(builder->getIndexType(), value));
    }

    void MLIRGenerator::insertComment(const std::string& comment) {
        mlir::ValueRange emptyRange{};
        builder->create<LLVM::UndefOp>(getNameLoc(comment),
                                       LLVM::LLVMVoidType::get(context),
                                       emptyRange,
                                       builder->getNamedAttr(comment, builder->getZeroAttr(builder->getI32Type())));
    }

    Location MLIRGenerator::getNameLoc(const std::string& name) {
        auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr("Query_1"), 0, 0);
        return NameLoc::get(builder->getStringAttr(name), baseLocation);
    }

    mlir::arith::CmpIPredicate convertToMLIRComparison(NES::PredicateOperation::BinaryOperatorType comparisonType) {
        //TODO extend to all comparison types
        switch (comparisonType) {
            case (NES::PredicateOperation::BinaryOperatorType::EQUAL_OP): return mlir::arith::CmpIPredicate::eq;
            case (NES::PredicateOperation::BinaryOperatorType::LESS_THAN_OP): return mlir::arith::CmpIPredicate::slt;
            case (NES::PredicateOperation::BinaryOperatorType::UNEQUAL_OP): return mlir::arith::CmpIPredicate::ne;
        }
    }

    //==-----------------------------------==//
    //==-- Create & Insert Functionality --==//
    //==-----------------------------------==//
    FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
                                                            uint8_t numResultBits,
                                                            std::vector<mlir::Type> argTypes,
                                                            bool varArgs) {
        // If function was declared already, return reference.
        if (theModule.lookupSymbol<LLVM::LLVMFuncOp>(name))
            return SymbolRefAttr::get(context, name);
        // Create function arg & result types (currently only int for result).
        LLVM::LLVMFunctionType llvmFnType;
        if (numResultBits != 0) {
            llvmFnType = LLVM::LLVMFunctionType::get(builder->getIntegerType(numResultBits), argTypes, varArgs);
        } else {
            llvmFnType = LLVM::LLVMFunctionType::get(LLVM::LLVMVoidType::get(context), argTypes, varArgs);
        }
        // The InsertionGuard saves the current IP and restores it after scope is left.
        PatternRewriter::InsertionGuard insertGuard(*builder);
        builder->restoreInsertionPoint(*globalInsertPoint);
        // Create function in global scope. Return reference.
        //Todo try other Linkage
        auto inlineAttr = builder->getNamedAttr("inline", builder->getStringAttr("alwaysinline"));
        auto attributes = ArrayRef<NamedAttribute>({inlineAttr});
        builder->create<LLVM::LLVMFuncOp>(theModule.getLoc(), name, llvmFnType, LLVM::Linkage::External, false);
        return SymbolRefAttr::get(context, name);
    }

    Value MLIRGenerator::createGlobalString(Location loc, StringRef name, StringRef value) {
        // Set insertion point to beginning of module for insertion of global string
        auto currentInsertionPoint = builder->saveInsertionPoint();
        builder->restoreInsertionPoint(*globalInsertPoint);

        // create and insert global string
        auto type = LLVM::LLVMArrayType::get(builder->getIntegerType(8), value.size());
        LLVM::GlobalOp global =
            builder->create<LLVM::GlobalOp>(loc, type, true, LLVM::Linkage::Internal, name, builder->getStringAttr(value), 0);

        // load global string into register
        builder->restoreInsertionPoint(currentInsertionPoint);
        Value globalPtr = builder->create<LLVM::AddressOfOp>(loc, global);

        auto globalString = builder->create<LLVM::GEPOp>(loc,
                                                         LLVM::LLVMPointerType::get(builder->getIntegerType(8)),
                                                         globalPtr,
                                                         ArrayRef<Value>({constZero, constZero}));
        return globalString;
    }

    void MLIRGenerator::insertValuePrint(mlir::Value printedValue, NES::Operation::BasicType type) {
        mlir::Value typeInt = getConstInt(getNameLoc("type"), 8, type);
        builder->create<LLVM::CallOp>(getNameLoc("printFromMLIR"),
                                      builder->getIntegerType(32),
                                      printfReference,
                                      ArrayRef<Value>({typeInt, printedValue}));
        // mlir::inlineCall()
    }


    //==---------------------------------==//
    //==-- MAIN WORK - Generating MLIR --==//
    //==---------------------------------==//
    MLIRGenerator::MLIRGenerator(mlir::MLIRContext& context) : context(&context) {
        builder = std::make_shared<OpBuilder>(&context);
        this->theModule = mlir::ModuleOp::create(getNameLoc("module"));

        // Generate execute function. Set input/output types and get its entry block.
        llvm::SmallVector<mlir::Type, 4> inputTypes(1, builder->getIntegerType(32));
        llvm::SmallVector<mlir::Type, 4> outputTypes(1, builder->getIntegerType(64));
        auto functionInOutTypes = builder->getFunctionType(inputTypes, outputTypes);

        // Create the 'execute' function which will be the entry point to the jit'd code.
        executeFunction = mlir::FuncOp::create(getNameLoc("EntryPoint"), "execute", functionInOutTypes);
        //avoid mangling
        executeFunction->setAttr("llvm.emit_c_interface", mlir::UnitAttr::get(&context));
        executeFunction.addEntryBlock();
        // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
        globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
        // Insert printFromMLIR function for debugging use.
        printfReference = insertExternalFunction("printFromMLIR", 32, {builder->getIntegerType(8)}, true);
        // Set InsertPoint to beginning of the execute function.
        builder->setInsertionPointToStart(&executeFunction.getBody().front());
        // Create 0 constant accessible in execute function scope
        constZero = getConstInt(getNameLoc("constant32Zero"), 32, 0);
        constOne = getConstInt(getNameLoc("const64One"), 64, 1);
    };

    mlir::ModuleOp MLIRGenerator::generateModuleFromNESIR(NES::NESIR* nesIR) {
        //Todo load TupleBuffer as int64 pointer
        auto externalDataSources = nesIR->getExternalDataSources();
        for(const auto& externalDataSource : externalDataSources) {
            if(externalDataSource->getExternalDataSourceType() == NES::ExternalDataSource::TupleBuffer) {
                createTupleBuffer(externalDataSource->getTypes(), externalDataSource->getIdentifier());
            }
        }
        generateMLIR(nesIR->getRootBlock());
        return theModule;
    }

    void MLIRGenerator::generateMLIR(NES::BasicBlockPtr basicBlock) {
        // Fixme: Instead of having a 'rootBlock' we have a rootOperation
        //  - We start generating MLIR from NESIR by calling generateMLIR(rootOperation)
        //   - an Operation can have BasicBlocks (LoopOperation, IfOperation)
        //    - if an Operation has a BasicBlock, we adjust the insertionPoint during MLIRGeneration
        //    - INTUITION: An Operation with a BasicBlock creates a new scope.
        for(const auto& operation : basicBlock->getOperations()) {
            generateMLIR(operation);
        }
    }

    Value MLIRGenerator::generateMLIR(const NES::OperationPtr& operation) {
        switch (operation->getOperationType()) {
            case NES::Operation::OperationType::LoopOp:
                return generateMLIR(std::static_pointer_cast<NES::LoopOperation>(operation));
            case NES::Operation::OperationType::ConstantOp:
                return generateMLIR(std::static_pointer_cast<NES::ConstantIntOperation>(operation));
            case NES::Operation::OperationType::AddOp:
                return generateMLIR(std::static_pointer_cast<NES::AddIntOperation>(operation));
            case NES::Operation::OperationType::LoadOp:
                return generateMLIR(std::static_pointer_cast<NES::LoadOperation>(operation));
            case NES::Operation::OperationType::AddressOp:
                return generateMLIR(std::static_pointer_cast<NES::AddressOperation>(operation));
            case NES::Operation::OperationType::StoreOp:
                return generateMLIR(std::static_pointer_cast<NES::StoreOperation>(operation));
            case NES::Operation::OperationType::PredicateOp:
                return generateMLIR(std::static_pointer_cast<NES::PredicateOperation>(operation));
        }
    }

    void MLIRGenerator::createTupleBuffer(std::vector<NES::Operation::BasicType> types, const std::string& identifier) {
        // Declare InputBuffer globally. Populate vector with MLIR types. Create Struct & array with type vector.
        printf("Types Size: %zu\n", types.size());
        //    std::vector<mlir::Type> structTypes;
        //    for (auto type : types) {
        //        structTypes.push_back(getMLIRType(type));
        //    }
        //    auto tupleStruct = LLVM::LLVMStructType::getLiteral(theModule.getContext(), structTypes, true);
        //    auto arrayType = LLVM::LLVMArrayType::get(tupleStruct, types.size());
        PatternRewriter::InsertionGuard insertGuard(*builder);
        builder->restoreInsertionPoint(*globalInsertPoint);
        //Todo handle via Map (could use llvm::StringMap?
//Todo save inputBuffer address as int -> maybe further down in hierarchy. Later use for pointer arithmetics
// - pointer to int ptoint and intopt
// - ALSO: use TupleBuffer* (e.g. int64*) as input to execute() function
        if(identifier == "inputBuffer") {
            inputBufferGlobal = builder->create<LLVM::GlobalOp>(getNameLoc("InputBuffer"),
                                                                builder->getIntegerType(8),
                                                                false,
                                                                LLVM::Linkage::External,
                                                                StringRef{identifier},
                                                                Attribute{});
        } else {
            outputBufferGlobal = builder->create<LLVM::GlobalOp>(getNameLoc("OutputBuffer"),
                                                                 builder->getIntegerType(8),
                                                                 false,
                                                                 LLVM::Linkage::External,
                                                                 StringRef{identifier},
                                                                 Attribute{});
        }
    }

    // Todo define entry point here. Is return correct?
    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoopOperation> loopOperation) {
        // Todo integrate initialization for root node:
        //  1. Load record via TupleBufferPtr
        //  2. 1 by 1, call 'generateMLIR()' on child operators of loopOperation
        //  - for now we assume that ALL operations are executed before accessing the next BasicBlock (no Ops after BasicBlock)
        // FIXME:
        //  - when to register TupleBuffer pointers with JIT instance?

        //==------------------------------==//
        //==--------- LOOP HEAD ----------==//
        //==------------------------------==//
// TOdo remove or move
        Value globalArrayPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), inputBufferGlobal);
        inputBufferIntPtr = builder->create<LLVM::PtrToIntOp>(getNameLoc("PtrToInt"), builder->getI64Type(), globalArrayPtr);

        // Define index variables for loop. Define the loop iteration variable
        Value iterationVariable = builder->create<LLVM::ConstantOp>(getNameLoc("iterationVariable"), builder->getI64Type(), builder->getI64IntegerAttr(0));
        auto lowerBound = builder->create<arith::ConstantIndexOp>(getNameLoc("lowerBound"), 0);
        auto upperBound = builder->create<arith::ConstantIndexOp>(getNameLoc("upperBound"), loopOperation->getUpperLimit());
        auto stepSize = builder->create<arith::ConstantIndexOp>(getNameLoc("stepSize"), 1);

        //==------------------------------==//
        //==--------- LOOP BODY ----------==//
        //==------------------------------==//
        insertComment("// Main For Loop");
        auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iterationVariable);

        // Set Insertion Point (IP) to inside of loop body. Process child node. Restore IP on return.
        auto currentInsertionPoint = builder->saveInsertionPoint();
        builder->setInsertionPointToStart(forLoop.getBody());
        currentRecordIdx = forLoop.getRegionIterArgs().begin()[0];
        for(auto operation: loopOperation->getOperations()) {
            generateMLIR(operation);
        }
        Value increasedIdx = builder->create<LLVM::AddOp>(getNameLoc("++iterationVar"), currentRecordIdx, constOne);
        builder->create<scf::YieldOp>(getNameLoc("yieldOperation"), increasedIdx);

        builder->restoreInsertionPoint(currentInsertionPoint);

        // Insert return into 'execute' function block. This is the FINAL return.
        builder->create<LLVM::ReturnOp>(getNameLoc("return"), forLoop->getResult(0));

        // Check if module is valid and return.
        theModule.push_back(executeFunction);
        if (failed(mlir::verify(theModule))) {
            theModule.emitError("module verification error");
            return nullptr;
        }
        return constZero;
    }

    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::ConstantIntOperation> constIntOp) {
        // Todo: implement CONST-INT operation
        printf("ConstIntValue: %ld\n", constIntOp->getConstantIntValue());
        //    builder->clearInsertionPoint();
        // TODO use dynamic type from NESIR
        Value constval =  builder->create<LLVM::ConstantOp>(getNameLoc("Constant Op"), builder->getIntegerType(8),
                                                           builder->getI8IntegerAttr(constIntOp->getConstantIntValue()));
        return constval;
    }
    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddIntOperation> addOp) {
        printf("Is add op: %d\n", addOp->classof(addOp.get()));

        //Todo cannot call RHS twice
        Value additionResult = builder->create<LLVM::AddOp>(getNameLoc("binOpResult"),
                                                            generateMLIR(addOp->getLHS()),
                                                            generateMLIR(addOp->getRHS()));
        //Todo remove print
        //    builder->clearInsertionPoint();
        auto printValFunc = insertExternalFunction("printValueFromMLIR", 0, {}, true);
        //        builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, printValFunc, currentRecordIdx);
        builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, printValFunc, additionResult);
        return additionResult;
    }

    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddressOperation> addressOp) {
        printf("Address Op field offset: %ld\n", addressOp->getFieldOffset());
        //    auto baseAddress = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), inputBufferGlobal);
        //    auto incrementedAddress = builder->create<LLVM::AddOp>(getNameLoc("increased Addr"), baseAddress,
        //                                                                                  getConstInt(getNameLoc("const 8"), 8, 8));
        //    return incrementedAddress;
        //Todo try to use intPTr
//        Value globalArrayPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), inputBufferGlobal);
//        Value indexWithOffset = builder->create<LLVM::MulOp>(getNameLoc("indexWithOffset"),
//                                                             getConstInt(getNameLoc("const 8"), 32, 8), currentRecordIdx);
        //    Value testIndex = builder->create<LLVM::MulOp>(getNameLoc("testIndex"), constOne, getConstInt(getNameLoc("const 8"), 32, 8));
        //    Value testIndex2 = builder->create<LLVM::MulOp>(getNameLoc("testIndex"),
        //                                                    builder->create<arith::ConstantOp>(getNameLoc("iterationVariable"), builder->getI32IntegerAttr(0)),
        //                                                        currentRecordIdx);

//        return builder->create<LLVM::GEPOp>(
//            getNameLoc("fieldAccess"),LLVM::LLVMPointerType::get(builder->getI8Type()),
//            globalArrayPtr,ArrayRef<Value>({ indexWithOffset }));
        Value fieldOffset = builder->create<LLVM::MulOp>(getNameLoc("fieldOffset"), currentRecordIdx, getConstInt(getNameLoc("1"), 64, 9));
//        Value offsetPlusOne = builder->create<LLVM::AddOp>(getNameLoc("plusOne"), fieldOffset, constOne);
        Value fieldIntIndex = builder->create<LLVM::AddOp>(getNameLoc("incIntIdx"), inputBufferIntPtr, fieldOffset);
//        Value fieldIntIndex = builder->create<LLVM::AddOp>(getNameLoc("incIntIdx"), fieldIntIndex, currentRecordIdx);
        return builder->create<LLVM::IntToPtrOp>(getNameLoc("currentIdxPtr"), LLVM::LLVMPointerType::get(builder->getI8Type()), fieldIntIndex);

    }

    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoadOperation> loadOp) {
        printf("Load Op.\n");
//        auto typePointer = LLVM::LLVMPointerType::get(builder->getI64Type());
        //    Value castedAddress = builder->create<LLVM::AddrSpaceCastOp>(getNameLoc("Casted Address"), typePointer,
        //                                                                 generateMLIR(loadOp->getAddressOp()));
        Value loadedVal = builder->create<LLVM::LoadOp>(getNameLoc("loadedValue"), generateMLIR(loadOp->getAddressOp()));
//        Value zextVal = builder->create<LLVM::ZExtOp>(getNameLoc("Zext"), builder->getI64Type(), loadedVal);


        return loadedVal;
    }

    Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::StoreOperation> storeOp) {
        // Todo: implement STORE operation
        printf("OutputBufferIdx[0]: %ld\n", storeOp->getOutputBufferIdx());

        Value fieldPointer = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), outputBufferGlobal);
//        Value castedAddress = builder->create<LLVM::AddrSpaceCastOp>(getNameLoc("Casted Address"),
//                                                                     LLVM::LLVMPointerType::get(builder->getI64Type()),
//                                                                     fieldPointer);
//        Value indexWithOffset = builder->create<LLVM::MulOp>(getNameLoc("indexWithOffset"),
//                                                             getConstInt(getNameLoc("const 8"), 32, 8), currentRecordIdx);
        auto outputPtr = builder->create<LLVM::GEPOp>(
            getNameLoc("fieldAccess"), LLVM::LLVMPointerType::get(builder->getI8Type()),
            fieldPointer, ArrayRef<Value>({ currentRecordIdx }));
        //    Value castedAddress = builder->create<LLVM::AddrSpaceCastOp>(getNameLoc("Casted Address"),
        //                                                                 LLVM::LLVMPointerType::get(builder->getI8Type()),
        //                                                                 fieldPointer);
        //    Value globalArrayPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), outputBufferGlobal);
        //    auto fieldPointer = builder->create<LLVM::GEPOp>(
        //        getNameLoc("obGEP"), LLVM::LLVMPointerType::get(getMLIRType(storeOp->getType())), //Todo use INT64 for resultType?
        //        //FIXME currentRecordIdx is not set
        //        globalArrayPtr,ArrayRef<Value>({constZero, currentRecordIdx,
        //                                                getConstInt(getNameLoc("Field Idx"), 64,
        //                                                storeOp->getOutputBufferIdx())}));
        builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), generateMLIR(storeOp->getValueToStore()), outputPtr);
        return constZero; // Todo: this should not return a value.
    }