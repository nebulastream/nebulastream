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

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <BaseUnitTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/SourceCreator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TestSinkDescriptor.hpp>
#include <Util/TestSourceDescriptor.hpp>
#include <utility>
#include <OpenCL/cl.h>

using namespace NES;
using Runtime::TupleBuffer;

#define NUMBER_OF_TUPLES 10

class OpenCLQueryExecutionTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("OpenCLQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG); }

  protected:
    void fillInputBuffer(Runtime::TupleBuffer& tupleBuffer,
                         const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout,
                         unsigned numberOfTuples) {
        // Insert input data into the tuple buffer.
        auto idField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, tupleBuffer);
        auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(1, memoryLayout, tupleBuffer);
        for (auto i = 0u; i < numberOfTuples; ++i) {
            idField[i] = i;
            valueField[i] = 1;
        }
        tupleBuffer.setNumberOfTuples(numberOfTuples);
    }

    void cleanUpPlan(Runtime::Execution::ExecutableQueryPlanPtr plan) {
        // TODO Why is this necessary?
        std::for_each(plan->getSources().begin(), plan->getSources().end(), [plan](auto source) {
            plan->notifySourceCompletion(source, Runtime::QueryTerminationType::Graceful);
        });
        std::for_each(plan->getPipelines().begin(), plan->getPipelines().end(), [plan](auto pipeline) {
            plan->notifyPipelineCompletion(pipeline, Runtime::QueryTerminationType::Graceful);
        });
        std::for_each(plan->getSinks().begin(), plan->getSinks().end(), [plan](auto sink) {
            plan->notifySinkCompletion(sink, Runtime::QueryTerminationType::Graceful);
        });
        ASSERT_TRUE(plan->stop());
    }
};

// TODO Documentation
#define ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, returnValue)                                                           \
    do {                                                                                                                         \
        if (status != CL_SUCCESS) {                                                                                              \
            NES_ERROR("{}: {}", message, status);                                                                                \
            return returnValue;                                                                                                  \
        }                                                                                                                        \
    } while (false)
#define ASSERT_OPENCL_SUCCESS(status, message) ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, true)
#define ASSERT_OPENCL_SUCCESS_OK(status, message) ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, ExecutionResult::Ok)

// TODO Documentation
class SimpleOpenCLPipelineStage : public Runtime::Execution::ExecutablePipelineStage {
    struct __attribute__((packed)) InputRecord {
      public:
        [[maybe_unused]] int32_t id;
        [[maybe_unused]] int32_t value;
    };

    struct __attribute__((packed)) OutputRecord {
      public:
        [[maybe_unused]] int32_t id;
        [[maybe_unused]] int32_t value;
        [[maybe_unused]] int32_t new1;
        [[maybe_unused]] int32_t new2;
    };

  public:
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Get kernel source
        auto kernelSourceFileName = std::string(TEST_DATA_DIRECTORY) + "computeNesMap.cl";
        auto kernelSourceFile = std::ifstream(kernelSourceFileName);
        auto kernelSource = std::string(std::istreambuf_iterator<char>(kernelSourceFile), std::istreambuf_iterator<char>());

        // Get OpenCL platform and device
        cl_int status;
        cl_uint numPlatforms = 0;
        status = clGetPlatformIDs(0, nullptr, &numPlatforms);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get number of OpenCL platforms");
        if (numPlatforms == 0) {
            NES_DEBUG("Did not find any OpenCL platforms.");
            return false;
        }

        auto platforms = std::vector<cl_platform_id>(numPlatforms);
        status = clGetPlatformIDs(numPlatforms, platforms.data(), nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get OpenCL platform IDs");

        for (auto i = 0u; i < numPlatforms; ++i) {
            char buffer[1024];
            status = clGetPlatformInfo(platforms[i], CL_PLATFORM_VENDOR, sizeof(buffer), buffer, nullptr);
            ASSERT_OPENCL_SUCCESS(status, "Failed to get platform vendor");
            std::stringstream platformInformation;
            platformInformation << i << ": " << platforms[i] << ": " << buffer << " ";
            status = clGetPlatformInfo(platforms[i], CL_PLATFORM_NAME, sizeof(buffer), buffer, nullptr);
            ASSERT_OPENCL_SUCCESS(status, "Failed to get platform name");
            platformInformation << buffer;
            NES_DEBUG("{}", platformInformation.str());
        }

        cl_uint numDevices = 0;
        cl_platform_id platform = platforms[0];
        NES_DEBUG("Using OpenCL platform: {}", static_cast<void*>(platform));

        status = clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL, 0, nullptr, &numDevices);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get number of devices");
        if (numDevices == 0) {
            NES_DEBUG("Did not find any OpenCL device.");
            return false;
        }

        auto devices = std::vector<cl_device_id>(numDevices);
        status = clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL, numDevices, devices.data(), nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Failed to get OpenCL device IDs");

        for (auto i = 0u; i < numDevices; ++i) {
            char buffer[1024];
            status = clGetDeviceInfo(devices[i], CL_DEVICE_NAME, sizeof(buffer), buffer, nullptr);
            ASSERT_OPENCL_SUCCESS(status, "Could not get device name");
            NES_DEBUG("{}: {}: {}", i, static_cast<void*>(devices[i]), buffer);
        }
        cl_device_id device = devices[0];
        NES_DEBUG("Using OpenCL device: {}", static_cast<void*>(device));

        // Create OpenCL context
        context = clCreateContext(nullptr, 1, &device, nullptr, nullptr, &status);
        ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL context");

        // Create OpenCL command queue
        commandQueue = clCreateCommandQueue(context, device, CL_QUEUE_PROFILING_ENABLE, &status);
        ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL command queue");

        // Compile kernel sources and create kernel.
        const char* kernelSourcePtr = kernelSource.c_str();
        program = clCreateProgramWithSource(context, 1, &kernelSourcePtr, nullptr, &status);
        ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL program");
        status = clBuildProgram(program, 1, &device, nullptr, nullptr, nullptr);
        ASSERT_OPENCL_SUCCESS(status, "Could not build OpenCL program");
        kernel = clCreateKernel(program, "computeNesMap", &status);
        ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL kernel");

        // Done
        return ExecutablePipelineStage::setup(pipelineExecutionContext);
    }

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        // Create OpenCL memory buffers.
        // Number of input tuples == number of output tuples for this kernel.
        auto numberOfTuples = buffer.getNumberOfTuples();
        auto inputSize = numberOfTuples * sizeof(InputRecord);
        auto outputSize = numberOfTuples * sizeof(OutputRecord);
        NES_DEBUG("numberOfTuples = {}; inputSize = {}; outputSize = {}", numberOfTuples, inputSize, outputSize);
        cl_int status;
        cl_mem inputDeviceBuffer = clCreateBuffer(context, CL_MEM_READ_WRITE, inputSize, nullptr, &status);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not create OpenCL device input buffer");
        cl_mem outputDeviceBuffer = clCreateBuffer(context, CL_MEM_READ_WRITE, outputSize, nullptr, &status);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not create OpenCL device output buffer");

        // Enqueue a write operation of the input to the device.
        cl_event writeEvent;
        auto input = buffer.getBuffer<InputRecord>();
//        auto input = (InputRecord*) malloc(inputSize);
//        cl_mem ddInput = clCreateBuffer(context, CL_MEM_ALLOC_HOST_PTR, inputSize, nullptr, &status);
//        ASSERT_OPENCL_SUCCESS_OK(status, "Could not create host input buffer");
//        auto input = (InputRecord*) clEnqueueMapBuffer(commandQueue, ddInput, CL_TRUE, CL_MAP_WRITE, 0, inputSize, 0, nullptr, nullptr, &status);
//        ASSERT_OPENCL_SUCCESS_OK(status, "Could not retrieve pointer to host input buffer");
//        for (unsigned i = 0; i < numberOfTuples; ++i) {
//            input[i].id = i;
//            input[i].value = i;
//        }
//        std::memcpy(input, buffer.getBuffer<InputRecord>(), inputSize);
        status = clEnqueueWriteBuffer(commandQueue, inputDeviceBuffer, CL_TRUE, 0, inputSize, input, 0, nullptr, &writeEvent);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not buffer write operation");
        status = clFinish(commandQueue);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not finish command queue after writing buffer");

        // Setup OpenCL kernel call.
        status = clSetKernelArg(kernel, 0, sizeof(cl_mem), &inputDeviceBuffer);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not set OpenCL kernel parameter (inputTuples)");
        status = clSetKernelArg(kernel, 1, sizeof(cl_mem), &outputDeviceBuffer);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not set OpenCL kernel parameter (resultTuples)");
        status = clSetKernelArg(kernel, 2, sizeof(cl_ulong), &numberOfTuples);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not set OpenCL kernel parameter (numberOfTuples)");

        // Enqueue the kernel.
        // TODO Use kernel wait list.
        cl_event executionEvent;
        size_t globalSize = numberOfTuples;
        status = clEnqueueNDRangeKernel(commandQueue, kernel, 1, nullptr, &globalSize, nullptr, 0, nullptr, &executionEvent);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not enqueue kernel execution");
        status = clWaitForEvents(1, &executionEvent);
        ASSERT_OPENCL_SUCCESS_OK(status, "Waiting for execution event failed");
        cl_ulong start;
        cl_ulong end;
        status = clGetEventProfilingInfo(executionEvent, CL_PROFILING_COMMAND_START, sizeof(cl_ulong), &start, nullptr);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not read kernel execution start time");
        status = clGetEventProfilingInfo(executionEvent, CL_PROFILING_COMMAND_END, sizeof(cl_ulong), &end, nullptr);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not read kernel execution end time");
        // Profiling information is in nanoseconds = 1e-9 seconds.
        NES_DEBUG("Kernel execution time = {} ms",  (end - start) / (1000 * 1000.0));
        status = clFinish(commandQueue);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not finish command queue after executing kernel");

        // Obtain an output buffer.
        auto outputBuffer = wc.allocateTupleBuffer();

        // This is a map kernel, so the number of output tuples == number of input tuples.
        outputBuffer.setNumberOfTuples(numberOfTuples);

        // Enqueue read operation.
        cl_event readEvent;
        auto output = outputBuffer.getBuffer<OutputRecord>();
//        auto output = (OutputRecord*) malloc(outputSize);
        status = clEnqueueReadBuffer(commandQueue, outputDeviceBuffer, CL_TRUE, 0, outputSize, output, 0, nullptr, &readEvent);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not enqueue read buffer operation");
        status = clFinish(commandQueue);
        ASSERT_OPENCL_SUCCESS_OK(status, "Could not finish command queue after reading output buffer");

        // Release memory objects
        clReleaseMemObject(inputDeviceBuffer);
        clReleaseMemObject(outputDeviceBuffer);

        // Emit the output buffer and return OK.
        ctx.emitBuffer(outputBuffer, wc);
        return ExecutionResult::Ok;
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Cleanup OpenCL resources
        clReleaseKernel(kernel);
        clReleaseProgram(program);
        clReleaseCommandQueue(commandQueue);
        clReleaseContext(context);
        // Done
        return ExecutablePipelineStage::stop(pipelineExecutionContext);
    }

  private:
    cl_context context;
    cl_program program;
    cl_kernel kernel;
    cl_command_queue commandQueue;
};

// TODO Documentation.
// TODO Extract steps
TEST_F(OpenCLQueryExecutionTest, simpleOpenCLKernel) {
    // Create an external operator with the custom OpenCL pipeline stage.
    auto customPipelineStage = std::make_shared<SimpleOpenCLPipelineStage>();
    // TODO Can this be nullptr?
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    // Create a worker configuration for the node engine and configure a physical source.
    auto workerConfiguration = WorkerConfiguration::create();
    auto sourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "default1", sourceType);
    workerConfiguration->physicalSources.add(physicalSource);

    // Create a node engine to execute the OpenCL pipeline.
    // TODO Is the query status listener necessary?
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                     .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                     .build();

    // Create the input schema with two uint32 fields.
    // TODO Why do the fields have to be prefixed with test$? If this is omitted, type inference below fails.
    auto inputSchema = Schema::create()->addField("test$id", BasicType::INT32)->addField("test$value", BasicType::INT32);

    // Create a source for the test.
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        inputSchema,
        [&](SchemaPtr schema,
            OperatorId operatorId,
            OriginId originId,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(schema,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 operatorId,
                                                                 originId,
                                                                 numSourceLocalBuffers,
                                                                 "testSource",
                                                                 std::move(successors));
        });

    // Create an output schema with the original fields and two new fields.
    auto outputSchema = Schema::create()
                            ->addField("id", BasicType::INT32)
                            ->addField("value", BasicType::INT32)
                            ->addField("new1", BasicType::INT32)
                            ->addField("new2", BasicType::INT32);

    // Create a sink for the test.
    const auto numberOfTuples = 256;
    auto testSink = std::make_shared<TestSink>(numberOfTuples, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    // Create a dummy query into which we can execute the custom pipeline.
    // TODO Why not use from(Schema) method?
    // TODO Can we do this without the filter?
    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 1000).sink(testSinkDescriptor);

    // Insert the custom pipeline stage into the query.
    auto typeInferencePhase =
        Optimizer::TypeInferencePhase::create(std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr()),
                                              Catalogs::UDF::UDFCatalog::create());
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];
    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    // Create an executable query plan.
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);

    // The plan should have two pipelines.
    ASSERT_EQ(plan->getPipelines().size(), 2u);

    // Create a worker context to execute the pipeline.
    // TODO What are these magic numbers?
    auto workerContext = Runtime::WorkerContext(1, nodeEngine->getBufferManager(), 4);

    // Get an input buffer from the buffer manager.
    // The buffer object has to be destroyed before nodeEngine->stop() is called, otherwise the following error occurs.
    // [LocalBufferPool.cpp:70] [destroy] NES Fatal Error on numberOfReservedBuffers == exclusiveBufferCount message: one or more buffers were not returned to the pool: 3 but expected 4
    {
        auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
        // !! converts to bool.
        // TupleBuffer has operator!() which tests if the internal ptr is set.
        // TODO This is bad design, it should be a explanatory method name, e.g., isValid.
        ASSERT_TRUE(!!buffer);

        // Fill the input buffer.
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
        fillInputBuffer(buffer, inputMemoryLayout, numberOfTuples);

        // Start the query plan.
        ASSERT_TRUE(plan->setup());
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
        ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
        // Execute the second pipeline, which is the custom pipeline?
        // TODO Why is it the second?
        ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

        // There should be one output buffer with 5 tuples.
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
        auto resultBuffer = testSink->get(0);
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), numberOfTuples);

        // Compare results
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());
        auto idField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, outputMemoryLayout, resultBuffer);
        auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(1, outputMemoryLayout, resultBuffer);
        auto new1Field = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(2, outputMemoryLayout, resultBuffer);
        auto new2Field = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(3, outputMemoryLayout, resultBuffer);
        for (auto i = 0u; i < numberOfTuples; ++i) {
            EXPECT_EQ(idField[i], i);
            EXPECT_EQ(valueField[i], 1);
            EXPECT_EQ(new1Field[i], idField[i] * 2);
            EXPECT_EQ(new2Field[i], idField[i] + 2);
        }
    }

    NES_DEBUG("Success");
    // Cleanup
    cleanUpPlan(plan);
    testSink->cleanupBuffers();
    // TODO Why is this necessary?
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
    ASSERT_TRUE(nodeEngine->stop());
}