///*
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        https://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//*/
//
//#include "mlir/Dialect/Arithmetic/IR/Arithmetic.h"
//#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
//#include "mlir/Dialect/LLVMIR/LLVMTypes.h"
//#include "mlir/IR/Attributes.h"
//#include "mlir/IR/Builders.h"
//#include "mlir/IR/BuiltinAttributes.h"
//#include "mlir/IR/BuiltinTypes.h"
//#include <Experimental/MLIR/NESAbstractionToMLIR.hpp>
//#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionForNode.hpp>
//#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionIfNode.hpp>
//#include <Experimental/NESAbstraction/NESAbstractionNode/NESAbstractionNode.hpp>
////#include "mlir/IR/Identifier.h"
//#include "mlir/IR/Location.h"
//#include "mlir/IR/MLIRContext.h"
//#include "mlir/IR/OpImplementation.h"
//#include "mlir/IR/Operation.h"
//#include "mlir/IR/PatternMatch.h"
//#include "mlir/IR/Value.h"
//#include "mlir/IR/Verifier.h"
//#include "mlir/Parser.h"
//#include "mlir/Support/LLVM.h"
////#include "mlir/Transforms/BufferUtils.h"
//#include "mlir/Transforms/InliningUtils.h"
//#include "llvm/ADT/Any.h"
//#include "llvm/ADT/StringRef.h"
//#include "llvm/Support/Casting.h"
//#include "llvm/Transforms/Utils/Cloning.h"
//#include <bits/floatn-common.h>
//#include <cstdint>
//#include <memory>
//#include <vector>
//
//using namespace mlir;
//
////==-----------------------==//
////==-- UTILITY FUNCTIONS --==//
////==-----------------------==//
//mlir::Type MLIRGenerator::getMLIRType(NESAbstractionNode::BasicType type) {
//    switch (type) {
//        case NESAbstractionNode::INT1:
//        case NESAbstractionNode::BOOLEAN: return builder->getIntegerType(1);
//        case NESAbstractionNode::INT8:
//        case NESAbstractionNode::CHAR: return builder->getIntegerType(8);
//        case NESAbstractionNode::INT16: return builder->getIntegerType(16);
//        case NESAbstractionNode::INT32: return builder->getIntegerType(32);
//        case NESAbstractionNode::INT64: return builder->getIntegerType(64);
//        case NESAbstractionNode::FLOAT: return Float32Type::get(context);
//        case NESAbstractionNode::DOUBLE: return Float64Type::get(context);
//        default: return builder->getIntegerType(32);
//    }
//}
//
//mlir::Value MLIRGenerator::getConstInt(Location loc, int numBits, int64_t value) {
//    return builder->create<LLVM::ConstantOp>(loc,
//                                             builder->getIntegerType(numBits),
//                                             builder->getIntegerAttr(builder->getIndexType(), value));
//}
//
//void MLIRGenerator::insertComment(const std::string& comment) {
//    mlir::ValueRange emptyRange{};
//    builder->create<LLVM::UndefOp>(getNameLoc(comment),
//                                   LLVM::LLVMVoidType::get(context),
//                                   emptyRange,
//                                   builder->getNamedAttr(comment, builder->getZeroAttr(builder->getI32Type())));
//}
//
//Location MLIRGenerator::getNameLoc(const std::string& name) {
//    auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr("Q" + queryId), 0, 0);
//    return NameLoc::get(builder->getStringAttr(name), baseLocation);
//}
//
//mlir::arith::CmpIPredicate convertToMLIRComparison(NESAbstractionIfNode::ComparisonType comparisonType) {
//    //TODO extend to all comparison types
//    switch (comparisonType) {
//        case (NESAbstractionIfNode::ComparisonType::SIGNED_INT_GT): return mlir::arith::CmpIPredicate::sgt;
//        case (NESAbstractionIfNode::ComparisonType::SIGNED_INT_GE): return mlir::arith::CmpIPredicate::sgt;
//        case (NESAbstractionIfNode::ComparisonType::SIGNED_INT_LT): return mlir::arith::CmpIPredicate::slt;
//        case (NESAbstractionIfNode::ComparisonType::SIGNED_INT_LE): return mlir::arith::CmpIPredicate::sle;
//        default: return mlir::arith::CmpIPredicate::sgt;
//    }
//}
//
////==-----------------------------------==//
////==-- Create & Insert Functionality --==//
////==-----------------------------------==//
//FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
//                                                        uint8_t numResultBits,
//                                                        std::vector<mlir::Type> argTypes,
//                                                        bool varArgs) {
//    // If function was declared already, return reference.
//    if (theModule.lookupSymbol<LLVM::LLVMFuncOp>(name))
//        return SymbolRefAttr::get(context, name);
//    // Create function arg & result types (currently only int for result).
//    LLVM::LLVMFunctionType llvmFnType;
//    if (numResultBits != 0) {
//        llvmFnType = LLVM::LLVMFunctionType::get(builder->getIntegerType(numResultBits), argTypes, varArgs);
//    } else {
//        llvmFnType = LLVM::LLVMFunctionType::get(LLVM::LLVMVoidType::get(context), argTypes, varArgs);
//    }
//    // The InsertionGuard saves the current IP and restores it after scope is left.
//    PatternRewriter::InsertionGuard insertGuard(*builder);
//    builder->restoreInsertionPoint(*globalInsertPoint);
//    // Create function in global scope. Return reference.
//    //Todo try other Linkage
//    auto inlineAttr = builder->getNamedAttr("inline", builder->getStringAttr("alwaysinline"));
//    auto attributes = ArrayRef<NamedAttribute>({inlineAttr});
//    builder->create<LLVM::LLVMFuncOp>(theModule.getLoc(), name, llvmFnType, LLVM::Linkage::External, false);
//    return SymbolRefAttr::get(context, name);
//}
//
//Value MLIRGenerator::createGlobalString(Location loc, StringRef name, StringRef value) {
//    // Set insertion point to beginning of module for insertion of global string
//    auto currentInsertionPoint = builder->saveInsertionPoint();
//    builder->restoreInsertionPoint(*globalInsertPoint);
//
//    // create and insert global string
//    auto type = LLVM::LLVMArrayType::get(builder->getIntegerType(8), value.size());
//    LLVM::GlobalOp global =
//        builder->create<LLVM::GlobalOp>(loc, type, true, LLVM::Linkage::Internal, name, builder->getStringAttr(value), 0);
//
//    // load global string into register
//    builder->restoreInsertionPoint(currentInsertionPoint);
//    Value globalPtr = builder->create<LLVM::AddressOfOp>(loc, global);
//
//    auto globalString = builder->create<LLVM::GEPOp>(loc,
//                                                     LLVM::LLVMPointerType::get(builder->getIntegerType(8)),
//                                                     globalPtr,
//                                                     ArrayRef<Value>({constZero, constZero}));
//    return globalString;
//}
//
//void MLIRGenerator::insertValuePrint(mlir::Value printedValue, NESAbstractionNode::BasicType type) {
//    mlir::Value typeInt = getConstInt(getNameLoc("type"), 8, type);
//    builder->create<LLVM::CallOp>(getNameLoc("printFromMLIR"),
//                                  builder->getIntegerType(32),
//                                  printfReference,
//                                  ArrayRef<Value>({typeInt, printedValue}));
//    // mlir::inlineCall()
//}
//
//Value MLIRGenerator::insertProxyCall(const std::string& funcName,
//                                     const std::string& ptrName,
//                                     NESAbstractionNode::BasicType returnType) {
//    // We cannot use LLVM::LLVMVoidType or MLIR::OpaqueType to get a void*.
//    // Instead we represent void* via int8*.
//    auto currentInsertionPoint = builder->saveInsertionPoint();
//    builder->restoreInsertionPoint(*globalInsertPoint);
//
//    // Create global function signature. Get Address of inserted object pointer.
//    auto proxyFunc = insertExternalFunction(funcName, 64, {}, true);
//    auto objectGEP = builder->create<LLVM::GlobalOp>(getNameLoc("TB Pointer"),
//                                                     LLVM::LLVMPointerType::get(builder->getIntegerType(8)),
//                                                     false,
//                                                     LLVM::Linkage::External,
//                                                     StringRef{ptrName},
//                                                     Attribute{});
//
//    // Load object address locally. Call proxy member function with objectPtr.
//    builder->restoreInsertionPoint(currentInsertionPoint);
//    Value objectPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadPtr"), objectGEP);
//    return builder->create<LLVM::CallOp>(getNameLoc("memberCall"), getMLIRType(returnType), proxyFunc, objectPtr).getResult(0);
//}
//
//void MLIRGenerator::insertTupleBuffers(std::vector<NESAbstractionNode::BasicType> types) {
//    // Populate vector with MLIR types. Create Struct & array with type vector.
//    std::vector<mlir::Type> structTypes;
//    for (auto type : types) {
//        structTypes.push_back(getMLIRType(type));
//    }
//    auto tupleStruct = LLVM::LLVMStructType::getLiteral(theModule.getContext(), structTypes, true);
//    auto arrayType = LLVM::LLVMArrayType::get(tupleStruct, types.size());
//
//    // Insert TupleBuffers globally.
//    PatternRewriter::InsertionGuard insertGuard(*builder);
//    builder->restoreInsertionPoint(*globalInsertPoint);
//    inputBufferGlobal = builder->create<LLVM::GlobalOp>(getNameLoc("TupleBuffer"),
//                                                        arrayType,
//                                                        false,
//                                                        LLVM::Linkage::External,
//                                                        StringRef{"IB"},
//                                                        Attribute{});
//    outputBufferGlobal = builder->create<LLVM::GlobalOp>(getNameLoc("OutputBuffer"),
//                                                         arrayType,
//                                                         false,
//                                                         LLVM::Linkage::External,
//                                                         StringRef{"OB"},
//                                                         Attribute{});
//}
//
//void MLIRGenerator::debugBufferPrint(mlir::Value currentTuple,
//                                     std::vector<uint64_t> indexes,
//                                     std::vector<NESAbstractionNode::BasicType> types) {
//
//    insertTupleBuffers(types);
//
//    Value result = insertProxyCall("getNumTuples", "IBPtr", NESAbstractionNode::BasicType::INT64);
//    auto printValFunc = insertExternalFunction("printValueFromMLIR", 0, {}, true);
//    builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, printValFunc, result);
//    Value globalArrayPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), inputBufferGlobal);
//    Value outputBufferPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), outputBufferGlobal);
//
//    for (int i = 0; i < (int) indexes.size(); ++i) {
//        mlir::Value fieldIdx = getConstInt(getNameLoc("fieldIdx"), 32, indexes.at(i));
//        auto ibGEP = builder->create<LLVM::GEPOp>(getNameLoc("ibGEP"),
//                                                  LLVM::LLVMPointerType::get(getMLIRType(types.at(i))),
//                                                  globalArrayPtr,
//                                                  ArrayRef<Value>({constZero, currentTuple, fieldIdx}));
//        auto obGEP = builder->create<LLVM::GEPOp>(getNameLoc("obGEP"),
//                                                  LLVM::LLVMPointerType::get(getMLIRType(types.at(i))),
//                                                  outputBufferPtr,
//                                                  ArrayRef<Value>({constZero, currentTuple, fieldIdx}));
//        mlir::Value loadedValue = builder->create<LLVM::LoadOp>(getNameLoc("loadedValue"), ibGEP);
//        builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), loadedValue, obGEP);
//        insertValuePrint(ibGEP, types.at(i));
//    }
//}
//
////==---------------------------------==//
////==-- MAIN WORK - Generating MLIR --==//
////==---------------------------------==//
//MLIRGenerator::MLIRGenerator(mlir::MLIRContext& context, std::string queryId) : context(&context) {
//    builder = std::make_shared<OpBuilder>(&context);
//    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));
//    this->queryId = queryId;
//
//    // Generate execute function. Set input/output types and get its entry block.
//    llvm::SmallVector<mlir::Type, 4> inputTypes(1, builder->getIntegerType(32));
//    llvm::SmallVector<mlir::Type, 4> outputTypes(1, builder->getIntegerType(32));
//    auto functionInOutTypes = builder->getFunctionType(inputTypes, outputTypes);
//
//    // Create the 'execute' function which will be the entry point to the jit'd code.
//    executeFunction = mlir::FuncOp::create(getNameLoc("EntryPoint"), "execute", functionInOutTypes);
//    //avoid mangling
//    executeFunction->setAttr("llvm.emit_c_interface", mlir::UnitAttr::get(&context));
//    executeFunction.addEntryBlock();
//    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
//    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
//    // Insert printFromMLIR function for debugging use.
//    printfReference = insertExternalFunction("printFromMLIR", 32, {builder->getIntegerType(8)}, true);
//    // Set InsertPoint to beginning of the execute function.
//    builder->setInsertionPointToStart(&executeFunction.getBody().front());
//    // Create 0 constant accessible in execute function scope
//    constZero = getConstInt(getNameLoc("constant32Zero"), 32, 0);
//};
//
//void MLIRGenerator::generateMLIRFromNode(std::shared_ptr<NESAbstractionNode> node, Value tuple) {
//    switch (node->getNodeType()) {
//        case NESAbstractionNode::ForNode: generateMLIRFromNode(std::static_pointer_cast<NESAbstractionForNode>(node)); break;
//        case NESAbstractionNode::IfNode: generateMLIRFromNode(std::static_pointer_cast<NESAbstractionIfNode>(node), tuple); break;
//        case NESAbstractionNode::BinOpNode:
//            generateMLIRFromNode(std::static_pointer_cast<NESAbstractionBinOpNode>(node), tuple);
//            break;
//        case NESAbstractionNode::WriteNode:
//            generateMLIRFromNode(std::static_pointer_cast<NESAbstractionWriteNode>(node), tuple);
//            break;
//        case NESAbstractionNode::ReadNode:
//            generateMLIRFromNode(std::static_pointer_cast<NESAbstractionBinOpNode>(node), tuple);
//            break;
//    }
//}
//
//void MLIRGenerator::generateMLIRFromNode(const std::shared_ptr<NESAbstractionForNode>& abstractionNode) {
//    // Define index variables for loop. Define the loop iteration variable.
//    auto lowerBound = builder->create<arith::ConstantIndexOp>(getNameLoc("lowerBound"), 0);
//    auto upperBound = builder->create<arith::ConstantIndexOp>(getNameLoc("upperBound"), abstractionNode->getUpperBound());
//    auto stepSize = builder->create<arith::ConstantIndexOp>(getNameLoc("stepSize"), 1);
//    Value iterationVariable = builder->create<arith::ConstantOp>(getNameLoc("iterationVariable"), builder->getI32IntegerAttr(0));
//    // Create an MLIR loop.
//    insertComment("// Main For Loop");
//    auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iterationVariable);
//
//    // Set IP to inside of loop body. Process child node. Restore IP on return.
//    auto currentInsertionPoint = builder->saveInsertionPoint();
//    builder->setInsertionPointToStart(forLoop.getBody());
//    debugBufferPrint(forLoop.getRegionIterArgs().begin()[0], abstractionNode->getIndexes(), abstractionNode->getTypes());
//    generateMLIRFromNode(std::move(abstractionNode->getChildNodes().front()), forLoop.getRegionIterArgs()[0]);
//    builder->restoreInsertionPoint(currentInsertionPoint);
//
//    // Insert return into 'execute' function block. This is the FINAL return.
//    builder->create<LLVM::ReturnOp>(getNameLoc("return"), forLoop->getResult(0));
//}
//
//void MLIRGenerator::generateMLIRFromNode(std::shared_ptr<NESAbstractionIfNode> abstractionNode, Value tuple) {
//    // Get comparisonValues and comparisons from the NESAbstractionNode
//    auto comparisonValues = abstractionNode->getComparisonValues();
//    auto comparisons = abstractionNode->getComparisons();
//
//    // Register comparisonValue and comparison in MLIR
//    Value comparisonValue =
//        builder->create<arith::ConstantOp>(getNameLoc("comparisonvalue"), builder->getI32IntegerAttr(comparisonValues.front()));
//
//    auto cmpOp = builder->create<arith::CmpIOp>(getNameLoc("comparison"),
//                                                convertToMLIRComparison(comparisons.front()),
//                                                tuple,
//                                                comparisonValue);
//
//    // Create IF Operation
//    auto ifOp = builder->create<scf::IfOp>(getNameLoc("ifOperation"), builder->getI32Type(), cmpOp, true);
//    // Get If and else child node. Set IP to if and then block. Restore IP after.
//    auto childVector = abstractionNode->getChildNodes();
//    auto currentInsertionPoint = builder->saveInsertionPoint();
//    builder->setInsertionPointToStart(ifOp.getThenBodyBuilder().getInsertionBlock());
//    generateMLIRFromNode(std::move(childVector.at(0)), tuple);
//    builder->setInsertionPointToStart(ifOp.getElseBodyBuilder().getInsertionBlock());
//    generateMLIRFromNode(std::move(childVector.at(1)), tuple);
//    builder->restoreInsertionPoint(currentInsertionPoint);
//
//    // Return result.
//    builder->create<scf::YieldOp>(getNameLoc("ifYield"), ifOp.getResult(0));
//}
//
//void MLIRGenerator::generateMLIRFromNode(std::shared_ptr<NESAbstractionBinOpNode> abstractionNode, Value tuple) {
//    Value elseReturnValue = builder->create<arith::ConstantOp>(getNameLoc("elseReturnValue"),
//                                                               builder->getI32IntegerAttr(abstractionNode->getLeftVal()));
//
//    // Since we are not using a TB yet, the FieldAccessOperand is not used, but we
//    // use the tuple value directly.
//    Value additionResult = builder->create<LLVM::AddOp>(getNameLoc("binOpResult"), elseReturnValue, tuple);
//    //Todo we should call read nodes in here to get the input from the TB.
//    //Todo covered by issues #44 and #46. This should also include IP handling.
//    generateMLIRFromNode(std::move(abstractionNode->getChildNodes().front()), additionResult);
//    builder->clearInsertionPoint();
//}
//
//void MLIRGenerator::generateMLIRFromNode(std::shared_ptr<NESAbstractionWriteNode> abstractionNode, Value tuple) {
//    // Todo abstractionNode is effectively not used
//    printf("NodeId: %ld\n", abstractionNode->getNodeId());
//    builder->create<scf::YieldOp>(getNameLoc("yieldOperation"), tuple);
//}
//
//mlir::ModuleOp MLIRGenerator::generateMLIR(std::shared_ptr<NESAbstractionNode> rootNode) {
//    // Recursively generate MLIR from nodes of tree. Tuple is first set
//    // during the for loop creation.
//    generateMLIRFromNode(std::move(rootNode), mlir::Value());
//
//    theModule.push_back(executeFunction);
//    if (failed(mlir::verify(theModule))) {
//        theModule.emitError("module verification error");
//        return nullptr;
//    }
//    return theModule;
//}