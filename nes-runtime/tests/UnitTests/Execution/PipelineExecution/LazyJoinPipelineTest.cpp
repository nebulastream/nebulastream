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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

namespace NES::Runtime::Execution {


class TestRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                          << callStack;

        NES_FATAL_ERROR(fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR(fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};


class LazyJoinPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {

  public:

    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LazyJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup LazyJoinPipelineTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup LazyJoinPipelineTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down LazyJoinPipelineTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down LazyJoinPipelineTest test class." << std::endl; }
};


TEST_P(LazyJoinPipelineTest, lazyJoinPipeline) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    auto memoryLayoutLeft = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bufferManager->getBufferSize());
    auto memoryLayoutRight = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bufferManager->getBufferSize());

    hier weiter machen mit dem erstellen der pipeline

    auto buildLeft = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true,
                                                                        lazyJoinSinkHelper.joinFieldNameLeft,
                                                                        lazyJoinSinkHelper.timeStampField,
                                                                        lazyJoinSinkHelper.leftSchema);
    auto buildRight = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ false,
                                                                 lazyJoinSinkHelper.joinFieldNameRight,
                                                                 lazyJoinSinkHelper.timeStampField,
                                                                 lazyJoinSinkHelper.rightSchema);


    NES_NOT_IMPLEMENTED();
}



INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        LazyJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<LazyJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

} // namespace NES::Runtime::Execution