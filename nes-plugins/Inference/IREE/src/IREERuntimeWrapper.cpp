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

#include "IREERuntimeWrapper.hpp"
#include <Util/Logger/Logger.hpp>
#include <iree/runtime/api.h>
#include <ErrorHandling.hpp>

namespace NES
{

void IREERuntimeWrapper::setup(iree_const_byte_span_t compiledModel)
{
    iree_runtime_instance_options_t instanceOptions;
    iree_runtime_instance_options_initialize(&instanceOptions);
    iree_runtime_instance_options_use_all_available_drivers(&instanceOptions);
    iree_runtime_instance_t* instance = nullptr;
    iree_status_t status = iree_runtime_instance_create(&instanceOptions, iree_allocator_system(), &instance);
    std::unique_ptr<iree_runtime_instance_t, decltype(&iree_runtime_instance_release)> runtimeInstance(instance, &iree_runtime_instance_release);

    iree_hal_device_t* device = nullptr;
    status = iree_runtime_instance_try_create_default_device(instance, iree_make_cstring_view("local-sync"), &device);
    std::unique_ptr<iree_hal_device_t, decltype(&iree_hal_device_release)> ireeDevice(device, &iree_hal_device_release);

    iree_runtime_session_options_t sessionOptions;
    iree_runtime_session_options_initialize(&sessionOptions);
    iree_runtime_session_t* session = nullptr;
    status = iree_runtime_session_create_with_device(
        instance, &sessionOptions, device, iree_runtime_instance_host_allocator(instance), &session);
    std::unique_ptr<iree_runtime_session_t, decltype(&iree_runtime_session_release)> ireeSession(session, &iree_runtime_session_release);

    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Setup failed. Could not create Session");
    }

    status = iree_runtime_session_append_bytecode_module_from_memory(session, compiledModel, iree_allocator_null());
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Setup failed. Could not Load model: {}", iree_status_code_string(iree_status_code(status)));
    }

    NES_DEBUG("Read the model from the bytecode buffer and set an IREE session up");
    this->instance = std::move(runtimeInstance);
    this->session = std::move(ireeSession);
    this->device = std::move(ireeDevice);
}

void IREERuntimeWrapper::execute(std::string functionName, void* inputData, size_t inputSize, void* outputData)
{
    iree_runtime_call_t call;
    // Cache the function after the first call such that initializing subsequent calls is cheaper
    if (this->function.module == nullptr)
    {
        auto status = iree_runtime_call_initialize_by_name(session.get(), iree_make_cstring_view(functionName.c_str()), &call);
        if (!iree_status_is_ok(status))
        {
            throw InferenceRuntime("Model Setup failed. Could not Load model");
        }
        this->function = call.function;
    }
    else
    {
        iree_runtime_call_initialize(session.get(), function, &call);
    }

    iree_hal_allocator_t* deviceAllocator = iree_runtime_session_device_allocator(session.get());
    iree_status_t status = iree_ok_status();

    iree_hal_buffer_view_t* view = nullptr;
    status = iree_hal_buffer_view_allocate_buffer_copy(
        device.get(),
        deviceAllocator,
        // Shape rank and dimensions:
        this->nDim,
        this->inputShape.data(),
        // Element type:
        IREE_HAL_ELEMENT_TYPE_FLOAT_32,
        // Encoding type:
        IREE_HAL_ENCODING_TYPE_DENSE_ROW_MAJOR,
        iree_hal_buffer_params_t{// Intended usage of the buffer (transfers, dispatches, etc):
                                 .usage = IREE_HAL_BUFFER_USAGE_DEFAULT,
                                 // Access to allow to this memory:
                                 .access = IREE_HAL_MEMORY_ACCESS_ALL,
                                 // Where to allocate (host or device):
                                 .type = IREE_HAL_MEMORY_TYPE_DEVICE_LOCAL,
                                 .queue_affinity = 0,
                                 .min_alignment = 0},
        // The actual heap buffer to wrap or clone and its allocator:
        iree_make_const_byte_span(inputData, inputSize),
        // Buffer view + storage are returned and owned by the caller:
        &view);
    std::unique_ptr<iree_hal_buffer_view_t, decltype(&iree_hal_buffer_view_release)> inputBuffer(view, &iree_hal_buffer_view_release);

    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Execution failed. Could not copy input tensor");
    }

    status = iree_runtime_call_inputs_push_back_buffer_view(&call, inputBuffer.get());
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Execution failed. Could not copy input tensor");
    }

    status = iree_runtime_call_invoke(&call, 0);
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Execution failed. Could not invoke model");
    }

    iree_hal_buffer_view_t* outputView = nullptr;
    status = iree_runtime_call_outputs_pop_front_buffer_view(&call, &outputView);
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Execution failed. Could not add output buffer");
    }
    std::unique_ptr<iree_hal_buffer_view_t, decltype(&iree_hal_buffer_view_release)> outputBuffer(
        outputView, &iree_hal_buffer_view_release);

    int outputSize = iree_hal_buffer_view_byte_length(outputBuffer.get());
    status = iree_hal_device_transfer_d2h(
        iree_runtime_session_device(session.get()),
        iree_hal_buffer_view_buffer(outputBuffer.get()),
        0,
        outputData,
        outputSize,
        IREE_HAL_TRANSFER_BUFFER_FLAG_DEFAULT,
        iree_infinite_timeout());

    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Execution failed. Could not copy output tensor");
    }

    iree_runtime_call_deinitialize(&call);
}

void IREERuntimeWrapper::setInputShape(std::vector<size_t> inputShape)
{
    this->inputShape = inputShape;
}

void IREERuntimeWrapper::setNDim(size_t nDim)
{
    this->nDim = nDim;
}

}
