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

//#include <Nautilus/Backends/WASM/WASMCompilationBackend.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <API/Schema.hpp>
#include <Execution/Expressions/ConstantIntegerExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
//#include <Nautilus/Backends/WASM/WAMRExecutionEngine.hpp>
#include "Execution/Expressions/WriteFieldExpression.hpp"
#include "Execution/Operators/Relational/Map.hpp"
#include "Nautilus/Backends/WASM/WASMRuntime.hpp"
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class WAMRExecutionTest : public Testing::NESBaseTest {
  public:
    //Runtime::Execution::ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    //std::shared_ptr<ExecutionEngine::Experimental::PipelineExecutionEngine> executionEngine;
    //std::shared_ptr<Nautilus::Backends::WASM::WASMCompilationBackend> backend;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WAMRExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WAMRExecutionTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_INFO("Setup SelectionPipelineTest test case.");
        //provider = Runtime::Execution::ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>(100);
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
        //backend = std::make_shared<Nautilus::Backends::WASM::WASMCompilationBackend>();
        //executionEngine = std::make_shared<ExecutionEngine::Experimental::PipelineExecutionEngine>(backend);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down WAMRExecutionTest test class."); }
};

TEST_F(WAMRExecutionTest, scanEmitTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto memoryProviderPtr2 = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProviderPtr));
    auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProviderPtr2));
    scanOperator->setChild(emitOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i);
        dynamicBuffer[i]["f2"].write((int64_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto mpeCtx = std::make_shared<Runtime::Execution::Operators::MockedPipelineExecutionContext>();

    //Timer timer("CompilationBasedPipelineExecutionEngine");
    //timer.start();

    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) &mpeCtx);
    pipelineExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto workerContextRef = Value<MemRef>((int8_t*) &wc);
    workerContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &buffer)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    auto rootOperator = pipeline->getRootOperator();

    auto executionTrace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Tracing::TraceContext::get();
        traceContext->addTraceArgument(pipelineExecutionContextRef.ref);
        traceContext->addTraceArgument(workerContextRef.ref);
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = Runtime::Execution::ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        rootOperator->open(ctx, recordBuffer);
        rootOperator->close(ctx, recordBuffer);
    });

    //NES_DEBUG(*executionTrace.get())
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    //NES_DEBUG(*executionTrace.get())
    //timer.snapshot("Trace Generation");
    auto ir = irCreationPhase.apply(executionTrace);
    //timer.snapshot("Nautilus IR Generation");
    NES_DEBUG(ir->toString())
    auto loweringProvider = std::make_unique<Nautilus::Backends::WASM::WASMCompiler>();
    auto loweringResult = loweringProvider->lower(ir);
    loweringResult->setArgs(wc, mpeCtx, std::make_shared<Runtime::TupleBuffer>(buffer));
    auto engine = std::make_unique<Backends::WASM::WASMRuntime>(loweringResult);
    engine->setup();
    engine->run();
    engine->close();
    
    //auto compilationBackend = std::make_unique<Backends::WASM::WASMCompilationBackend>();
    //auto engine = compilationBackend->compile(ir);
}

TEST_F(WAMRExecutionTest, selectionTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto memoryProviderPtr2 = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProviderPtr));
    auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProviderPtr2));

    auto readF1 = std::make_shared<Runtime::Execution::Expressions::ConstantIntegerExpression>(5);
    auto readF2 = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>("f1");
    auto equalsExpression = std::make_shared<Runtime::Execution::Expressions::EqualsExpression>(readF1, readF2);
    auto selectionOperator = std::make_shared<Runtime::Execution::Operators::Selection>(equalsExpression);
    scanOperator->setChild(selectionOperator);
    selectionOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto mpeCtx = std::make_shared<Runtime::Execution::Operators::MockedPipelineExecutionContext>();

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i);
        dynamicBuffer[i]["f2"].write((int64_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    //Timer timer("CompilationBasedPipelineExecutionEngine");
    //timer.start();

    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) &mpeCtx);
    pipelineExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto workerContextRef = Value<MemRef>((int8_t*) &wc);
    workerContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &buffer)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    auto rootOperator = pipeline->getRootOperator();

    auto executionTrace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Tracing::TraceContext::get();
        traceContext->addTraceArgument(pipelineExecutionContextRef.ref);
        traceContext->addTraceArgument(workerContextRef.ref);
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = Runtime::Execution::ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        rootOperator->open(ctx, recordBuffer);
        rootOperator->close(ctx, recordBuffer);
    });

    //NES_DEBUG(*executionTrace.get())
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    //NES_DEBUG(*executionTrace.get())
    //timer.snapshot("Trace Generation");
    auto ir = irCreationPhase.apply(executionTrace);
    //timer.snapshot("Nautilus IR Generation");
    NES_DEBUG(ir->toString())
    auto loweringProvider = std::make_unique<Nautilus::Backends::WASM::WASMCompiler>();
    auto loweringResult = loweringProvider->lower(ir);
    loweringResult->setArgs(wc, mpeCtx, std::make_shared<Runtime::TupleBuffer>(buffer));
    auto engine = std::make_unique<Backends::WASM::WASMRuntime>(loweringResult);
    engine->setup();
    engine->run();
    engine->close();
}

TEST_F(WAMRExecutionTest, mapTest) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto memoryProviderPtr2 = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProviderPtr));
    auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProviderPtr2));

    auto constInt = std::make_shared<Runtime::Execution::Expressions::ConstantIntegerExpression>(5);
    auto readF1 = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>("f1");
    auto addEx = std::make_shared<Runtime::Execution::Expressions::AddExpression>(readF1, constInt);
    auto writeEx = std::make_shared<Runtime::Execution::Expressions::WriteFieldExpression>("f2", addEx);

    auto mapOperator = std::make_shared<Runtime::Execution::Operators::Map>(writeEx);

    scanOperator->setChild(mapOperator);
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);
    auto mpeCtx = std::make_shared<Runtime::Execution::Operators::MockedPipelineExecutionContext>();

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer.getCapacity(); i++) {
        dynamicBuffer[i]["f1"].write((int64_t) i);
        dynamicBuffer[i]["f2"].write((int64_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) mpeCtx.get());
    pipelineExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto workerContextRef = Value<MemRef>((int8_t*) wc.get());
    workerContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &buffer)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    auto rootOperator = pipeline->getRootOperator();

    auto executionTrace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Tracing::TraceContext::get();
        traceContext->addTraceArgument(pipelineExecutionContextRef.ref);
        traceContext->addTraceArgument(workerContextRef.ref);
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = Runtime::Execution::ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        rootOperator->open(ctx, recordBuffer);
        rootOperator->close(ctx, recordBuffer);
    });
    //NES_DEBUG(*executionTrace.get())
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    //NES_DEBUG(*executionTrace.get())
    //timer.snapshot("Trace Generation");
    auto ir = irCreationPhase.apply(executionTrace);
    //timer.snapshot("Nautilus IR Generation");
    NES_DEBUG(ir->toString())
    auto loweringProvider = std::make_unique<Nautilus::Backends::WASM::WASMCompiler>();
    auto loweringResult = loweringProvider->lower(ir);
    loweringResult->setArgs(wc, mpeCtx, std::make_shared<Runtime::TupleBuffer>(buffer));
    auto engine = std::make_unique<Backends::WASM::WASMRuntime>(loweringResult);
    engine->setup();
    engine->run();
    engine->close();

    auto resultBuffer = mpeCtx->buffers[0];
    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < resultDynamicBuffer.getCapacity(); i++) {
        NES_INFO(resultDynamicBuffer[i]["f1"].read<int64_t>())
        NES_INFO(resultDynamicBuffer[i]["f2"].read<int64_t>())
        ASSERT_EQ(resultDynamicBuffer[i]["f2"].read<int64_t>(), 5);
    }

    auto dynamicBuffer2 = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < dynamicBuffer2.getCapacity(); i++) {
        NES_INFO(dynamicBuffer2[i]["f1"].read<int64_t>())
        NES_INFO(dynamicBuffer2[i]["f2"].read<int64_t>())
    }
}

}// namespace NES::Nautilus
