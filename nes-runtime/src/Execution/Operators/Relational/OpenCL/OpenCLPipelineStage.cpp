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


#include <Execution/Operators/Relational/OpenCL/OpenCLPipelineStage.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#ifdef ENABLE_OPENCL
#ifdef __APPLE__
#include <OpenCL/cl.h>
#else
#include <CL/cl.h>
#endif
#else
// Define some OpenCL types, so that this header file compiles, and we don't need #ifdef's everywhere.
using cl_platform_id = unsigned;
using cl_device_id = unsigned;
#endif
namespace NES::Runtime::Execution {

#define ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, returnValue)                                                           \
    do {                                                                                                                         \
        if (status != CL_SUCCESS) {                                                                                              \
            NES_ERROR("{}: {}", message, status);                                                                                \
            return returnValue;                                                                                                  \
        }                                                                                                                        \
    } while (false)
#define ASSERT_OPENCL_SUCCESS(status, message) ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, ExecutionResult::Error)
#define ASSERT_OPENCL_SUCCESS2(status, message) ASSERT_OPENCL_SUCCESS_AND_RETURN(status, message, 1)

OpenCLPipelineStage::OpenCLPipelineStage(const std::string& openCLKernelCode,
                                         const size_t deviceIdIndex,
                                         uint64_t inputRecordSize,
                                         uint64_t outputRecordSize)
    : ExecutablePipelineStage(), openCLKernelCode(openCLKernelCode), deviceIdIndex(deviceIdIndex),
      inputRecordSize(inputRecordSize), outputRecordSize(outputRecordSize) {}

uint32_t OpenCLPipelineStage::setup(NES::Runtime::Execution::PipelineExecutionContext&) {
    // Retrieving the OpenCL device ID from a new OpenCLManager assumes that the order in which platforms and devices are
    // enumerated by the OpenCL runtime is stable.
    OpenCLManager openCLManager;
    auto openCLDevice = openCLManager.getDevices()[deviceIdIndex];
    NES_DEBUG("Using OpenCL device: {}", openCLDevice.deviceInfo.deviceName);
    deviceId = openCLDevice.deviceId;

    // Create OpenCL context
    cl_int status;
    context = clCreateContext(nullptr, 1, &deviceId, nullptr, nullptr, &status);
    ASSERT_OPENCL_SUCCESS2(status, "Could not create OpenCL context");

    // Create OpenCL command queue
    commandQueue = clCreateCommandQueue(context, deviceId, CL_QUEUE_PROFILING_ENABLE, &status);
    ASSERT_OPENCL_SUCCESS2(status, "Could not create OpenCL command queue");

    // Compile kernel sources and create kernel.
    const char* kernelSourcePtr = openCLKernelCode.c_str();
    program = clCreateProgramWithSource(context, 1, &kernelSourcePtr, nullptr, &status);
    ASSERT_OPENCL_SUCCESS2(status, "Could not create OpenCL program");
    status = clBuildProgram(program, 1, &deviceId, nullptr, nullptr, nullptr);
    ASSERT_OPENCL_SUCCESS2(status, "Could not build OpenCL program");
    // TODO How to determine the kernel name?
    kernel = clCreateKernel(program, "computeNesMap", &status);
    ASSERT_OPENCL_SUCCESS2(status, "Could not create OpenCL kernel");
    return 0;
}

ExecutionResult OpenCLPipelineStage::execute(NES::Runtime::TupleBuffer& inputTupleBuffer,
                                             NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                             NES::Runtime::WorkerContext& workerContext) {
    // Create OpenCL memory buffers.
    // Number of input tuples == number of output tuples for this kernel.
    auto numberOfTuples = inputTupleBuffer.getNumberOfTuples();
    auto inputSize = numberOfTuples * inputRecordSize;
    auto outputSize = numberOfTuples * outputRecordSize;
    NES_DEBUG("numberOfTuples = {}; inputSize = {}; outputSize = {}", numberOfTuples, inputSize, outputSize);
    cl_int status;
    cl_mem inputDeviceBuffer = clCreateBuffer(context, CL_MEM_READ_WRITE, inputSize, nullptr, &status);
    ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL device input buffer");
    cl_mem outputDeviceBuffer = clCreateBuffer(context, CL_MEM_READ_WRITE, outputSize, nullptr, &status);
    ASSERT_OPENCL_SUCCESS(status, "Could not create OpenCL device output buffer");

    // Enqueue a write operation of the input to the device.
    cl_event writeEvent;
    status = clEnqueueWriteBuffer(commandQueue,
                                  inputDeviceBuffer,
                                  CL_TRUE,
                                  0,
                                  inputSize,
                                  inputTupleBuffer.getBuffer<>(),
                                  0,
                                  nullptr,
                                  &writeEvent);
    ASSERT_OPENCL_SUCCESS(status, "Could not buffer write operation");
    status = clFinish(commandQueue);
    ASSERT_OPENCL_SUCCESS(status, "Could not finish command queue after writing buffer");

    // Setup OpenCL kernel call.
    status = clSetKernelArg(kernel, 0, sizeof(cl_mem), &inputDeviceBuffer);
    ASSERT_OPENCL_SUCCESS(status, "Could not set OpenCL kernel parameter (inputTuples)");
    status = clSetKernelArg(kernel, 1, sizeof(cl_mem), &outputDeviceBuffer);
    ASSERT_OPENCL_SUCCESS(status, "Could not set OpenCL kernel parameter (resultTuples)");
    status = clSetKernelArg(kernel, 2, sizeof(cl_ulong), &numberOfTuples);
    ASSERT_OPENCL_SUCCESS(status, "Could not set OpenCL kernel parameter (numberOfTuples)");

    // Enqueue the kernel.
    // TODO Use kernel wait list.
    cl_event executionEvent;
    size_t globalSize = numberOfTuples;
    status = clEnqueueNDRangeKernel(commandQueue, kernel, 1, nullptr, &globalSize, nullptr, 0, nullptr, &executionEvent);
    ASSERT_OPENCL_SUCCESS(status, "Could not enqueue kernel execution");
    status = clWaitForEvents(1, &executionEvent);
    ASSERT_OPENCL_SUCCESS(status, "Waiting for execution event failed");
    cl_ulong start;
    cl_ulong end;
    status = clGetEventProfilingInfo(executionEvent, CL_PROFILING_COMMAND_START, sizeof(cl_ulong), &start, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not read kernel execution start time");
    status = clGetEventProfilingInfo(executionEvent, CL_PROFILING_COMMAND_END, sizeof(cl_ulong), &end, nullptr);
    ASSERT_OPENCL_SUCCESS(status, "Could not read kernel execution end time");
    // Profiling information is in nanoseconds = 1e-9 seconds.
    NES_DEBUG("Kernel execution time = {} ms", (end - start) / (1000 * 1000.0));
    status = clFinish(commandQueue);
    ASSERT_OPENCL_SUCCESS(status, "Could not finish command queue after executing kernel");

    // Obtain an output buffer.
    auto outputBuffer = workerContext.allocateTupleBuffer();

    // This is a map kernel, so the number of output tuples == number of input tuples.
    outputBuffer.setNumberOfTuples(numberOfTuples);

    // Enqueue read operation.
    cl_event readEvent;
    status = clEnqueueReadBuffer(commandQueue,
                                 outputDeviceBuffer,
                                 CL_TRUE,
                                 0,
                                 outputSize,
                                 outputBuffer.getBuffer(),
                                 0,
                                 nullptr,
                                 &readEvent);
    ASSERT_OPENCL_SUCCESS(status, "Could not enqueue read buffer operation");
    status = clFinish(commandQueue);
    ASSERT_OPENCL_SUCCESS(status, "Could not finish command queue after reading output buffer");

    // Release memory objects
    clReleaseMemObject(inputDeviceBuffer);
    clReleaseMemObject(outputDeviceBuffer);

    // Emit the output buffer and return OK.
    pipelineExecutionContext.emitBuffer(outputBuffer, workerContext);
    return ExecutionResult::Ok;
}

uint32_t OpenCLPipelineStage::stop(Runtime::Execution::PipelineExecutionContext&) {
    // Cleanup OpenCL resources
    clReleaseKernel(kernel);
    clReleaseProgram(program);
    clReleaseCommandQueue(commandQueue);
    clReleaseContext(context);
    // Done
    return 0;
}

}// namespace NES::Runtime::Execution