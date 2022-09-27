// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at

//         https://www.apache.org/licenses/LICENSE-2.0

//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// */

// #include <Experimental/MLIR/MLIRUtility.hpp>
// #include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
// #include <Nautilus/IR/Operations/AddressOperation.hpp>
// #include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
// #include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
// #include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
// #include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
// #include <Nautilus/IR/Operations/BranchOperation.hpp>
// #include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
// #include <Nautilus/IR/Operations/ConstIntOperation.hpp>
// #include <Nautilus/IR/Operations/FunctionOperation.hpp>
// #include <Nautilus/IR/Operations/IfOperation.hpp>
// #include <Nautilus/IR/Operations/LoadOperation.hpp>
// #include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
// #include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
// #include <Nautilus/IR/Operations/Operation.hpp>
// #include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
// #include <Nautilus/IR/Operations/ReturnOperation.hpp>
// #include <Nautilus/IR/Operations/StoreOperation.hpp>
// #include <Runtime/BufferManager.hpp>
// #include <Runtime/TupleBuffer.hpp>
// #include <Util/Logger/Logger.hpp>
// #include <gtest/gtest.h>

// namespace NES {
// namespace NES::Nautilus::IR {
// namespace Operations {
// class MLIRGeneratorArithmeticOpsTest : public testing::Test {
//   public:
//     NES::ExecutionEngine::Experimental::MLIR::MLIRUtility* mlirUtility;
//     /* Will be called before any test in this class are executed. */
//     static void SetUpTestCase() {
//         NES::Logger::setupLogging("MLIRGeneratorIfTest.log", NES::LogLevel::LOG_DEBUG);
//         NES_INFO("Setup MLIRGeneratorIfTest test case.");
//     }

//     void SetUp() override { mlirUtility = new NES::ExecutionEngine::Experimental::MLIR::MLIRUtility("", false); }

//     /* Will be called before a test is executed. */
//     void TearDown() override { NES_INFO("Tear down MLIRGeneratorIfTest test case."); }

//     /* Will be called after all tests in this class are finished. */
//     static void TearDownTestCase() { NES_INFO("Tear down MLIRGeneratorIfTest test class."); }


// };

// void printBuffer(std::vector<Operations::PrimitiveStamp> types, uint64_t numTuples, int8_t* bufferPointer) {
//     for (uint64_t i = 0; i < numTuples; ++i) {
//         printf("------------\nTuple Nr. %lu\n------------\n", i + 1);
//         for (auto type : types) {
//             switch (type) {
//                 case PrimitiveStamp::INT1: {
//                     printf("Value(INT1): %d \n", *bufferPointer);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case PrimitiveStamp::INT8: {
//                     printf("Value(INT8): %d \n", *bufferPointer);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case PrimitiveStamp::INT16: {
//                     int16_t* value = (int16_t*) bufferPointer;
//                     printf("Value(INT16): %d \n", *value);
//                     bufferPointer += 2;
//                     break;
//                 }
//                 case PrimitiveStamp::INT32: {
//                     int32_t* value = (int32_t*) bufferPointer;
//                     printf("Value(INT32): %d \n", *value);
//                     bufferPointer += 4;
//                     break;
//                 }
//                 case PrimitiveStamp::INT64: {
//                     int64_t* value = (int64_t*) bufferPointer;
//                     printf("Value(INT64): %ld \n", *value);
//                     bufferPointer += 8;
//                     break;
//                 }
//                 case PrimitiveStamp::UINT8: {
//                     uint8_t* value = (uint8_t*) bufferPointer;
//                     printf("Value(UINT8): %u \n", *value);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case PrimitiveStamp::UINT16: {
//                     uint16_t* value = (uint16_t*) bufferPointer;
//                     printf("Value(UINT16): %u \n", *value);
//                     bufferPointer += 2;
//                     break;
//                 }
//                 case PrimitiveStamp::UINT32: {
//                     uint32_t* value = (uint32_t*) bufferPointer;
//                     printf("Value(UINT32): %u \n", *value);
//                     bufferPointer += 4;
//                     break;
//                 }
//                 case PrimitiveStamp::UINT64: {
//                     uint64_t* value = (uint64_t*) bufferPointer;
//                     printf("Value(UINT64): %lu \n", *value);
//                     bufferPointer += 8;
//                     break;
//                 }
//                 case PrimitiveStamp::BOOLEAN: {
//                     bool* value = (bool*) bufferPointer;
//                     printf("Value(BOOL): %s \n", (*value) ? "true" : "false");
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case PrimitiveStamp::CHAR: {
//                     char* value = (char*) bufferPointer;
//                     printf("Value(CHAR): %c \n", *value);
//                     bufferPointer += 1;
//                     break;
//                 }
//                 case PrimitiveStamp::FLOAT: {
//                     float* value = (float*) bufferPointer;
//                     printf("Value(FLOAT): %f \n", *value);
//                     bufferPointer += 4;
//                     break;
//                 }
//                 case PrimitiveStamp::DOUBLE: {
//                     double* value = (double*) bufferPointer;
//                     printf("Value(DOUBLE): %f \n", *value);
//                     bufferPointer += 8;
//                     break;
//                 }
//                 default: break;
//             }
//         }
//     }
// }

// /*
// std::shared_ptr<ProxyCallOperation> getProxyCallOperation(ProxyCallOperation::ProxyCallType proxyCallType, bool getInputTB) {

//     std::vector<PrimitiveStamp> proxyDataBufferReturnArgs{PrimitiveStamp::INT8PTR};
//     std::vector<std::string> proxyCallOpArgs{"inputTupleBuffer"};
//     switch (proxyCallType) {
//         case ProxyCallOperation::GetDataBuffer: {
//             std::string proxyCallOpName = "getInDataBufOp";
//             if (getInputTB) {
//                 proxyCallOpName = "getOutDataBufOp";
//                 proxyCallOpArgs = {"outputTupleBuffer"};
//             }
//             return std::make_shared<ProxyCallOperation>(Operation::GetDataBuffer,
//                                                         proxyCallOpName,
//                                                         proxyCallOpArgs,
//                                                         proxyDataBufferReturnArgs,
//                                                         PrimitiveStamp::INT8PTR);
//         }
//         case ProxyCallOperation::GetNumTuples: {
//             return std::make_shared<ProxyCallOperation>(Operation::GetNumTuples,
//                                                         "getNumTuplesOp",
//                                                         proxyCallOpArgs,
//                                                         proxyDataBufferReturnArgs,
//                                                         PrimitiveStamp::INT64);
//         }
//         case ProxyCallOperation::SetNumTuples: {
//             return std::make_shared<ProxyCallOperation>(Operation::SetNumTuples,
//                                                         "setNumTuplesOp",
//                                                         std::vector<std::string>{"outputTupleBuffer", "getNumTuplesOp"},
//                                                         proxyDataBufferReturnArgs,
//                                                         PrimitiveStamp::INT64);
//         }
//         case ProxyCallOperation::Other: {
//             return nullptr;
//         }
//     }
// }
//  */

// template<typename... Args>
// BasicBlockPtr createBB(std::string identifier, int level, std::vector<PrimitiveStamp> argTypes, Args... args) {
//     std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments;
//     std::vector<std::string> argList({args...});
//     for (uint64_t i = 0; i < argTypes.size(); i++) {
//         arguments.emplace_back(std::make_shared<Operations::BasicBlockArgument>(argList[i], argTypes[i]));
//     }
//     return std::make_shared<BasicBlock>(identifier, level, std::vector<OperationPtr>{}, arguments);
// }

// BasicBlockPtr
// saveBB(BasicBlockPtr basicBlock, std::unordered_map<std::string, BasicBlockPtr>& savedBBs, const std::string& basicBlockName) {
//     savedBBs.emplace(std::pair{basicBlockName, basicBlock});
//     return savedBBs[basicBlockName];
// }

// /**
//  * @brief Test MLIR Generation for Add Int Operation
//  *
//  * int64Const1 = 42
//  * int64Const2 = 58
//  * addOperation = int64Const1 + int64Const2
//  * return addOperation
//  */
// TEST_F(MLIRGeneratorArithmeticOpsTest, testAddIntOperation) {
//     auto nesIR = std::make_shared<NESIR>();
//     auto rootBasicBlock = createBB("executeBodyBB", 0, {});
//     auto int64Const1 = std::make_shared<ConstIntOperation>("int64Const1", 42, 64);
//     rootBasicBlock->addOperation(int64Const1);
//     auto int64Const2 = std::make_shared<ConstIntOperation>("int64Const2", 58, 64);
//     rootBasicBlock->addOperation(int64Const2);
//     auto addOp = std::make_shared<AddOperation>("addOperation", int64Const1, int64Const2);
//     rootBasicBlock->addOperation(addOp);
//     auto returnOperation = std::make_shared<ReturnOperation>(addOp);
//     rootBasicBlock->addOperation(returnOperation);

//     std::vector<PrimitiveStamp> executeArgTypes{};
//     std::vector<std::string> executeArgNames{};
//     nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
//         ->addFunctionBasicBlock(rootBasicBlock);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
//     assert(loadedModuleSuccess == 0);
//     auto engine = mlirUtility->prepareEngine();
//     auto function = (int (*)()) engine->lookup("execute").get();
//     int64_t resultValue = function();
//     ASSERT_EQ(resultValue, 100);
// }

// /**
//  * @brief Test MLIR Generation for Add Float Operation
//  *
//  * float64Const1 = 42.01
//  * float64Const2 = 58.05
//  * addOperation = float64Const1 + float64Const2
//  * return addOperation
//  */
// TEST_F(MLIRGeneratorArithmeticOpsTest, testAddFloatOperation) {
//     auto nesIR = std::make_shared<NESIR>();
//     auto rootBasicBlock = createBB("executeBodyBB", 0, {});
//     auto float64Const1 = std::make_shared<ConstFloatOperation>("float64Const1", 42.01, 64);
//     rootBasicBlock->addOperation(float64Const1);
//     auto float64Const2 = std::make_shared<ConstFloatOperation>("float64Const2", 58.05, 64);
//     rootBasicBlock->addOperation(float64Const2);
//     auto addOp = std::make_shared<AddOperation>("addOperation", float64Const1, float64Const2);
//     rootBasicBlock->addOperation(addOp);
//     auto returnOperation = std::make_shared<ReturnOperation>(addOp);
//     rootBasicBlock->addOperation(returnOperation);

//     std::vector<PrimitiveStamp> executeArgTypes{};
//     std::vector<std::string> executeArgNames{};
//     nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, DOUBLE))
//         ->addFunctionBasicBlock(rootBasicBlock);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
//     assert(loadedModuleSuccess == 0);
//     auto engine = mlirUtility->prepareEngine();
//     auto function = (double (*)()) engine->lookup("execute").get();
//     double resultValue = function();
//     ASSERT_EQ(resultValue, 100.06);
// }

// // TODO ADD TESTS FOR OTHER ARITHMETICAL OPERATIONS

// /*
//  * @brief Here, we dump the string representation for a simple NESIR containing an AddOperation.

// TEST_F(MLIRGeneratorArithmeticOpsTest, printSimpleNESIRwithAddOperation) {
//     const uint64_t numTuples = 2;
//     auto buffMgr = std::make_shared<NES::Runtime::BufferManager>();
//     // Create InputBuffer and fill it with data.
//     auto inputBuffer = buffMgr->getBufferBlocking();
//     struct __attribute__((packed)) testTupleStruct {
//         int64_t d;
//         uint64_t h;
//         double j;
//     };
//     testTupleStruct testTuplesArray[numTuples] = {testTupleStruct{64, INT64_MAX, 4.2}, testTupleStruct{-64, 64, -4.2}};
//     auto inputBufferPointer = inputBuffer.getBuffer<testTupleStruct>();
//     memcpy(inputBufferPointer, &testTuplesArray, sizeof(testTupleStruct) * numTuples);
//     inputBuffer.setNumberOfTuples(numTuples);
//     auto outputBuffer = buffMgr->getBufferBlocking();

//     //Debug Print
//     auto proxyPrintOp = std::make_shared<ProxyCallOperation>(Operation::ProxyCallType::Other,
//                                                              "printValueFromMLIR",
//                                                              std::vector<std::string>{"iOp"},
//                                                              std::vector<PrimitiveStamp>{},
//                                                              PrimitiveStamp::VOID);

//     //Setup: Execute function args - A map that allows to save and reuse defined BBs - NESIR module.
//     std::vector<PrimitiveStamp> executeArgTypes{INT8PTR, INT8PTR};
//     std::vector<std::string> executeArgNames{"inputTupleBuffer", "outputTupleBuffer"};
//     std::unordered_map<std::string, BasicBlockPtr> savedBBs;
//     auto nesIR = std::make_shared<NESIR>();

//     nesIR->addRootOperation(std::make_shared<FunctionOperation>("execute", executeArgTypes, executeArgNames, INT64))
//         ->addFunctionBasicBlock(
//             createBB("executeBodyBB", 0, {})
//                 ->addOperation(std::make_shared<ConstIntOperation>("iOp", 0, 64))
//                 ->addOperation(std::make_shared<ConstIntOperation>("const1Op", 1, 64))
//                 ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, true))
//                 ->addOperation(getProxyCallOperation(Operation::GetDataBuffer, false))
//                 ->addOperation(getProxyCallOperation(Operation::GetNumTuples, false))
//                 ->addOperation(getProxyCallOperation(Operation::SetNumTuples, false))
//                 ->addOperation(make_shared<LoopOperation>(LoopOperation::ForLoop, std::vector<std::string>{"iOp"}))
//                 ->addLoopHeadBlock(
//                     saveBB(createBB("loopHeadBB", 1, {}, "iOp"), savedBBs, "loopHeadBB")
//                         ->addOperation(std::make_shared<CompareOperation>("loopHeadCompareOp",
//                                                                           "iOp",
//                                                                           "getNumTuplesOp",
//                                                                           CompareOperation::ISLT))
//                         ->addOperation(make_shared<IfOperation>("loopHeadCompareOp",
//                                                                 std::vector<std::string>{"iOp"},
//                                                                 std::vector<std::string>{"iOp"}))
//                         ->addThenBlock(
//                             createBB("loopBodyBB", 2, {}, "iOp")
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("inBufAddrOp4", INT64, 24, 0, "iOp", "getInDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("inBufAddrOp8", UINT64, 24, 8, "iOp", "getInDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("inBufAddrOp10", DOUBLE, 24, 16, "iOp", "getInDataBufOp"))
//                                 ->addOperation(std::make_shared<LoadOperation>("int64", "inBufAddrOp4"))
//                                 ->addOperation(std::make_shared<LoadOperation>("uint64", "inBufAddrOp8"))
//                                 ->addOperation(std::make_shared<LoadOperation>("double", "inBufAddrOp10"))

//                                 ->addOperation(std::make_shared<ConstIntOperation>("int64Const1", 65,  64))
//                                 ->addOperation(
//                                     std::make_shared<SubOperation>("subInt64", "int64", "int64Const1", PrimitiveStamp::INT64))
//                                 ->addOperation(
//                                     std::make_shared<SubOperation>("subUInt64", "uint64", "int64Const1", PrimitiveStamp::UINT64))
//                                 ->addOperation(std::make_shared<ConstIntOperation>("int64Const2", -3,  64))
//                                 ->addOperation(
//                                     std::make_shared<MulOperation>("mulInt64", "int64", "int64Const2", PrimitiveStamp::INT64))
//                                 ->addOperation(
//                                     std::make_shared<DivOperation>("divInt64", "int64", "int64Const2", PrimitiveStamp::INT64))
//                                 ->addOperation(std::make_shared<ConstIntOperation>("uint64Const2", 3,  64))
//                                 ->addOperation(
//                                     std::make_shared<DivOperation>("divUInt64", "uint64", "uint64Const2", PrimitiveStamp::UINT64))
//                                 ->addOperation(std::make_shared<ConstFloatOperation>("doubleConst", -4.2,  64))
//                                 ->addOperation(
//                                     std::make_shared<MulOperation>("mulDouble", "double", "doubleConst", PrimitiveStamp::DOUBLE))
//                                 ->addOperation(
//                                     std::make_shared<DivOperation>("divDouble", "double", "doubleConst", PrimitiveStamp::DOUBLE))
//                                 ->addOperation(
//                                     std::make_shared<SubOperation>("subDouble", "double", "doubleConst", PrimitiveStamp::DOUBLE))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("outBufAddrOp0", INT64, 64, 0, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("outBufAddrOp1", UINT64, 64, 8, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("outBufAddrOp3", INT64, 64, 16, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("outBufAddrOp4", INT64, 64, 24, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(
//                                     std::make_shared<AddressOperation>("outBufAddrOp8", UINT64, 64, 32, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(std::make_shared<
//                                                AddressOperation>("outBufAddrOp10", DOUBLE, 64, 40, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(std::make_shared<
//                                                AddressOperation>("outBufAddrOp12", DOUBLE, 64, 48, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(std::make_shared<
//                                                AddressOperation>("outBufAddrOp13", DOUBLE, 64, 56, "iOp", "getOutDataBufOp"))
//                                 ->addOperation(std::make_shared<StoreOperation>("subInt64", "outBufAddrOp0"))
//                                 ->addOperation(std::make_shared<StoreOperation>("subUInt64", "outBufAddrOp1"))
//                                 ->addOperation(std::make_shared<StoreOperation>("mulInt64", "outBufAddrOp3"))
//                                 ->addOperation(std::make_shared<StoreOperation>("divInt64", "outBufAddrOp4"))
//                                 ->addOperation(std::make_shared<StoreOperation>("divUInt64", "outBufAddrOp8"))
//                                 ->addOperation(std::make_shared<StoreOperation>("mulDouble", "outBufAddrOp10"))
//                                 ->addOperation(std::make_shared<StoreOperation>("divDouble", "outBufAddrOp12"))
//                                 ->addOperation(std::make_shared<StoreOperation>("subDouble", "outBufAddrOp13"))

//                                 ->addOperation(std::make_shared<BranchOperation>(std::vector<std::string>{"loopEndIncIOp"}))
//                                 ->addNextBlock(savedBBs["loopHeadBB"]))
//                         ->addElseBlock(createBB("executeReturnBB", 1, {})->addOperation(std::make_shared<ReturnOperation>(0)))));


//     // NESIR to MLIR
//     auto mlirUtility = new NES::ExecutionEngine::Experimental::MLIR::MLIRUtility("", false);
//     int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(nesIR);
//     assert(loadedModuleSuccess == 0);

//     mlirUtility->runJit(false, std::addressof(inputBuffer), std::addressof(outputBuffer));

//     // Print OutputBuffer after execution.
//     std::vector<PrimitiveStamp> types{INT64, UINT64, INT64, INT64, UINT64, DOUBLE, DOUBLE, DOUBLE};
//     auto outputBufferPointer = outputBuffer.getBuffer<int8_t>();
//     printBuffer(types, numTuples, outputBufferPointer);
//     NES_DEBUG("Number of tuples in OutputTupleBuffer: " << outputBuffer.getNumberOfTuples());
// }*/

// }// namespace Operations
// }// namespace NES::Nautilus::IR
// }// namespace NES
