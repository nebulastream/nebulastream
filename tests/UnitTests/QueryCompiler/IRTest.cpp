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

#include "Util/SharedLibrary.hpp"
#include <Compiler/Util/SharedLibrary.hpp>
#include <QueryCompiler/IR/Expressions/AddNode.hpp>
#include <QueryCompiler/IR/Expressions/ConstantNode.hpp>
#include <QueryCompiler/IR/IfNode.hpp>
#include <QueryCompiler/IR/LoopNode.hpp>
#include <QueryCompiler/IR/Node.hpp>
#include <QueryCompiler/Interpreter/Values/NesBool32.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

using namespace std;
using namespace std;

namespace NES::QueryCompilation {

class IRTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("IRTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup IRTest test class.");
    }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down QueryCompilerTest Test."); }
};

/**
 * @brief Create IR for addition:
 * 10 + 12
 */
TEST_F(IRTest, AdditionIR) {
    auto v1 = IR::ConstantNode::create(std::make_shared<NesInt32>(10));
    auto v2 = IR::ConstantNode::create(std::make_shared<NesInt32>(12));
    auto add = IR::AddNode::create(v1, v2);
    ASSERT_EQ(v1, add->getLeft());
    ASSERT_EQ(v2, add->getRight());
}

/**
 * @brief Create IR for addition:
 * if(true) {
 *
 * } else {
 *
 * }
 */
TEST_F(IRTest, IfIRNode) {
    auto v1 = IR::ConstantNode::create(std::make_shared<NesBool>(true));
    auto ifNode = IR::IFNode::create(v1);
    auto trueSuccessor = ifNode->getTrueSuccessor();
    auto v2 = IR::ConstantNode::create(std::make_shared<NesInt32>(12));
    trueSuccessor->addNode(IR::AddNode::create(v1, v2));
}

/**
 * @brief Create IR for addition:
 *
 * for(int i = 0 ; i< 10; i++)
 *
 *
 */
TEST_F(IRTest, LoopIrNode) {
    auto lower = IR::ConstantNode::create(std::make_shared<NesInt32>(1));
    auto upper = IR::ConstantNode::create(std::make_shared<NesInt32>(1));
    auto step = IR::ConstantNode::create(std::make_shared<NesInt32>(1));
    auto loopNode = IR::LoopNode::create(lower, upper, step);
}

template<class U, U>
struct IntegralConstant {};
template<class U, U* u>
std::string mangledSymbolName() {
    std::string null = typeid(IntegralConstant<U*, nullptr>).name();
    std::string symbol = typeid(IntegralConstant<U*, u>).name();
    return symbol.substr(null.size() - 3, symbol.size() - null.size() + 0);
}
template<typename Function>
class ProxyCall {
  private:
    std::string functionName();
    void* shared_lib;
};


TEST_F(IRTest, InvokeIrNode) {
    typedef uint64_t (Runtime::TupleBuffer::*Some_fnc_ptr)() const;
    Some_fnc_ptr fnc_ptr = &Runtime::TupleBuffer::getNumberOfTuples;
    std::cout << typeid(Some_fnc_ptr).name() << std::endl;

    std::cout << typeid(fnc_ptr).name() << std::endl;
    std::cout << typeid(&Runtime::TupleBuffer::getOriginId).name() << std::endl;
    std::cout << typeid(&Runtime::TupleBuffer::getWatermark).name() << std::endl;
    std::cout << typeid(&Runtime::TupleBuffer::getBufferSize).name() << std::endl;
    std::cout << typeid(&Runtime::TupleBuffer::getNumberOfTuples).name() << std::endl;
    //Runtime::TupleBuffer sc;
    //(sc.*fnc_ptr)();
   /* std::function<uint64_t (Runtime::TupleBuffer::*)() const> f = &Runtime::TupleBuffer::getNumberOfTuples;
    std::cout << typeid(&test).name() << std::endl;
    std::cout << typeid(fnc_ptr).name() << std::endl;
    std::cout << typeid(fnc_ptr).name() << std::endl;
    auto shared = SharedLibrary::load("/home/pgrulich/projects/nes/nebulastream/cmake-build-debug/libnes.so");
    auto func = shared->getSymbol("MN3NES7Runtime11TupleBufferEKFmvE");
    std::cout << func << std::endl;
    ProxyCall("Runtime::TupleBuffer::getNumberOfTuples", fnc_ptr)
    //std::cout << mangledSymbolName<decltype(Runtime::TupleBuffer), Runtime::TupleBuffer::setNumberOfTuples>() << std::endl;
    */
}

}// namespace NES::QueryCompilation