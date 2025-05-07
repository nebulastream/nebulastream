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

namespace NES::Runtime::Execution::Operators
{

void IREERuntimeWrapper::setup(iree_const_byte_span_t compiledModel)
{
    iree_runtime_instance_options_t instanceOptions;
    iree_runtime_instance_options_initialize(&instanceOptions);
    iree_runtime_instance_options_use_all_available_drivers(&instanceOptions);
    iree_runtime_instance_t* instance = nullptr;
    iree_status_t status = iree_runtime_instance_create(&instanceOptions, iree_allocator_system(), &instance);
    NES_DEBUG("Created IREE runtime instance")

    iree_hal_device_t* device = nullptr;
    iree_runtime_instance_try_create_default_device(instance, iree_make_cstring_view("local-task"), &device);
    NES_DEBUG("Created IREE device")

    iree_runtime_session_options_t sessionOptions;
    iree_runtime_session_options_initialize(&sessionOptions);
    iree_runtime_session_t* session = nullptr;
    status = iree_runtime_session_create_with_device(
        instance, &sessionOptions, device, iree_runtime_instance_host_allocator(instance), &session);
    iree_hal_device_release(device);
    NES_DEBUG("Created IREE session")

    if (iree_status_is_ok(status))
    {
        status = iree_runtime_session_append_bytecode_module_from_memory(session, compiledModel, iree_allocator_null());
        if (iree_status_is_ok(status))
        {
            NES_DEBUG("Read the model from the bytecode buffer");
        }
        else
        {
            std::exit(1);
        }
    }
    this->instance = instance;
    this->session = session;
    this->device = device;
}

void IREERuntimeWrapper::execute(std::string functionName, void* inputData, size_t inputSize, void* outputData)
{
    iree_runtime_call_t call;
    // Cache the function after the first call such that initializing subsequent calls is cheaper
    if (this->function.module == nullptr)
    {
        auto status = iree_runtime_call_initialize_by_name(session, iree_make_cstring_view(functionName.c_str()), &call);
        if (!iree_status_is_ok(status))
        {
            exit(1);
        }
        this->function = call.function;
    }
    else
    {
        iree_runtime_call_initialize(session, function, &call);
    }

    device = iree_runtime_session_device(session);
    iree_hal_allocator_t* deviceAllocator = iree_runtime_session_device_allocator(session);
    iree_status_t status = iree_ok_status();

    if (iree_status_is_ok(status))
    {
        iree_hal_buffer_view_t* inputBuffer = nullptr;
        iree_hal_dim_t* inputBufferShape = (iree_hal_dim_t*)malloc(sizeof(int) * nDim);

        for (int i = 0; i < nDim; i++)
        {
            inputBufferShape[i] = inputShape[i];
        }

        status = iree_hal_buffer_view_allocate_buffer_copy(
            device,
            deviceAllocator,
            // Shape rank and dimensions:
            // this->inputShapeSize, inputBufferShape,
            this->nDim,
            inputBufferShape,
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
            &inputBuffer);
        if (iree_status_is_ok(status))
        {
            status = iree_runtime_call_inputs_push_back_buffer_view(&call, inputBuffer);
        }
        iree_hal_buffer_view_release(inputBuffer);

        if (iree_status_is_ok(status))
        {
            status = iree_runtime_call_invoke(&call, /*flags=*/0);
        }

        iree_hal_buffer_view_t* outputBuffer = nullptr;
        if (iree_status_is_ok(status))
        {
            status = iree_runtime_call_outputs_pop_front_buffer_view(&call, &outputBuffer);
        }

        if (outputData != nullptr)
        {
            free(outputData);
        }

        if (iree_status_is_ok(status))
        {
            int outputSize = iree_hal_buffer_view_byte_length(outputBuffer);
            outputData = (float*)malloc(outputSize);
            iree_hal_device_transfer_d2h(
                iree_runtime_session_device(session),
                iree_hal_buffer_view_buffer(outputBuffer),
                0,
                outputData,
                outputSize,
                IREE_HAL_TRANSFER_BUFFER_FLAG_DEFAULT,
                iree_infinite_timeout());
        }
        iree_hal_buffer_view_release(outputBuffer);
    }
    iree_runtime_call_deinitialize(&call);
}

void IREERuntimeWrapper::setInputShape(std::vector<int> inputShape)
{
    this->inputShape = inputShape;
}

void IREERuntimeWrapper::setNDim(int nDim)
{
    this->nDim = nDim;
}

}
