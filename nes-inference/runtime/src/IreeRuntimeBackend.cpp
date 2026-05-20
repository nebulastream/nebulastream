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

#include <IreeRuntimeBackend.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <iree/runtime/api.h>
#include <ErrorHandling.hpp>
#include <Model.hpp>
#include <RuntimeBackend.hpp>
#include <scope_guard.hpp>

namespace
{
std::string ireeStatusString(iree_status_t status)
{
    char* output = nullptr;
    size_t size = 0;
    const iree_allocator_t allocator = iree_allocator_system();
    iree_status_to_string(status, &allocator, &output, &size);
    std::string result(output, size);
    iree_allocator_free(allocator, output);
    return result;
}

void ireeCheckStatus(iree_status_t status, const char* msg)
{
    if (!iree_status_is_ok(status))
    {
        throw NES::InferenceRuntimeFailure(fmt::format("{}: {}", msg, ireeStatusString(status)));
    }
}
}

namespace NES
{
RuntimeMetadata IreeRuntimeBackend::setup(const CompiledModel& model)
{
    iree_runtime_instance_options_t instanceOptions;
    iree_runtime_instance_options_initialize(&instanceOptions);
    iree_runtime_instance_options_use_all_available_drivers(&instanceOptions);

    iree_runtime_instance_t* inst = nullptr;
    iree_status_t status = iree_runtime_instance_create(&instanceOptions, iree_allocator_system(), &inst);
    ireeCheckStatus(status, "Model Setup failed. Could not create IREE runtime instance");
    std::unique_ptr<iree_runtime_instance_t, InstanceDeleter> instPtr(inst);

    NES_DEBUG("Created IREE runtime instance")
    iree_hal_device_t* dev = nullptr;
    status = iree_runtime_instance_try_create_default_device(instPtr.get(), iree_make_cstring_view("local-sync"), &dev);
    ireeCheckStatus(status, "Model Setup failed. Could not create IREE device");
    /// Session takes a reference to the device, so we release our reference
    SCOPE_EXIT
    {
        iree_hal_device_release(dev);
    };

    NES_DEBUG("Created IREE device")
    iree_runtime_session_options_t sessionOptions;
    iree_runtime_session_options_initialize(&sessionOptions);
    iree_runtime_session_t* sess = nullptr;
    status = iree_runtime_session_create_with_device(
        instPtr.get(), &sessionOptions, dev, iree_runtime_instance_host_allocator(instPtr.get()), &sess);
    ireeCheckStatus(status, "Model Setup failed. Could not create Session");
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> sessPtr(sess);

    NES_DEBUG("Read the model from the bytecode buffer");
    const iree_const_byte_span_t byteCodeSpan{
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) IREE API requires const uint8_t* but bytecode is stored as std::byte
        .data = reinterpret_cast<const uint8_t*>(model.getData().data()),
        .data_length = model.getData().size()};
    status = iree_runtime_session_append_bytecode_module_from_memory(sessPtr.get(), byteCodeSpan, iree_allocator_null());
    ireeCheckStatus(status, "Model Setup failed. Could not Load model");

    instance = std::move(instPtr);
    session = std::move(sessPtr);
    inputShape = model.getInputShape();
    nDim = model.getNDim();
    functionName = model.getFunctionName();

    return RuntimeMetadata{
        .inputShape = inputShape,
        .nDim = nDim,
        .functionName = functionName,
        .inputSize = model.inputSize(),
        .outputSize = model.outputSize()};
}

void IreeRuntimeBackend::infer(std::byte* inputBuffer, size_t inputBufferSize, std::byte* outputBuffer, size_t outputBufferSize)
{
    iree_runtime_call_t call;
    if (function.module == nullptr)
    {
        auto* status = iree_runtime_call_initialize_by_name(session.get(), iree_make_cstring_view(functionName.c_str()), &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize function call");
        function = call.function;
    }
    else
    {
        auto* status = iree_runtime_call_initialize(session.get(), function, &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize cached function call");
    }
    SCOPE_EXIT
    {
        iree_runtime_call_deinitialize(&call);
    };

    iree_hal_device_t* dev = iree_runtime_session_device(session.get());
    iree_hal_allocator_t* deviceAllocator = iree_runtime_session_device_allocator(session.get());

    iree_hal_buffer_view_t* view = nullptr;
    iree_status_t status = iree_hal_buffer_view_allocate_buffer_copy(
        dev,
        deviceAllocator,
        nDim,
        inputShape.data(),
        IREE_HAL_ELEMENT_TYPE_FLOAT_32,
        IREE_HAL_ENCODING_TYPE_DENSE_ROW_MAJOR,
        iree_hal_buffer_params_t{
            .usage = IREE_HAL_BUFFER_USAGE_DEFAULT,
            .access = IREE_HAL_MEMORY_ACCESS_ALL,
            .type = IREE_HAL_MEMORY_TYPE_DEVICE_LOCAL,
            .queue_affinity = 0,
            .min_alignment = 0},
        iree_make_const_byte_span(inputBuffer, inputBufferSize),
        &view);
    ireeCheckStatus(status, "Model Execution failed. Could not copy input tensor");
    SCOPE_EXIT
    {
        iree_hal_buffer_view_release(view);
    };

    status = iree_runtime_call_inputs_push_back_buffer_view(&call, view);
    ireeCheckStatus(status, "Model Execution failed. Could not push input tensor");

    status = iree_runtime_call_invoke(&call, 0);
    ireeCheckStatus(status, "Model Execution failed. Could not invoke model");

    iree_hal_buffer_view_t* outputView = nullptr;
    status = iree_runtime_call_outputs_pop_front_buffer_view(&call, &outputView);
    ireeCheckStatus(status, "Model Execution failed. Could not pop output buffer");
    SCOPE_EXIT
    {
        iree_hal_buffer_view_release(outputView);
    };

    auto outputSizeBytes = iree_hal_buffer_view_byte_length(outputView);
    if (outputSizeBytes > outputBufferSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Model output size {} B exceeds buffer capacity {} B", outputSizeBytes, outputBufferSize);
    }
    status = iree_hal_device_transfer_d2h(
        iree_runtime_session_device(session.get()),
        iree_hal_buffer_view_buffer(outputView),
        0,
        outputBuffer,
        outputSizeBytes,
        IREE_HAL_TRANSFER_BUFFER_FLAG_DEFAULT,
        iree_infinite_timeout());

    ireeCheckStatus(status, "Model Execution failed. Could not copy output tensor");
}
}
