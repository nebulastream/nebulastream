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

#include <Inference/IREERuntimeWrapper.hpp>

#include <cstddef>
#include <span>
#include <string>
#include <Util/Logger/Logger.hpp>
#include <iree/runtime/api.h>
#include <ErrorHandling.hpp>
#include <scope_guard.hpp>

namespace NES::Inference
{

inline void ireeCheckStatus(iree_status_t status, const char* msg)
{
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime(msg);
    }
}

IREERuntimeWrapper::IREERuntimeWrapper(IREERuntimeWrapper&& other) noexcept
    : inputShape(std::move(other.inputShape)), nDim(other.nDim), instance(std::move(other.instance)), session(std::move(other.session)),
      function(other.function)
{
    other.function = {};
}

IREERuntimeWrapper& IREERuntimeWrapper::operator=(IREERuntimeWrapper&& other) noexcept
{
    if (this != &other)
    {
        inputShape = std::move(other.inputShape);
        nDim = other.nDim;
        session = std::move(other.session);
        instance = std::move(other.instance);
        function = other.function;

        other.function = {};
    }
    return *this;
}

void IREERuntimeWrapper::setup(iree_const_byte_span_t compiledModel)
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
    if (!iree_status_is_ok(status) || dev == nullptr)
    {
        throw InferenceRuntime("Model Setup failed. Could not create IREE device");
    }
    NES_DEBUG("Created IREE device")

    iree_runtime_session_options_t sessionOptions;
    iree_runtime_session_options_initialize(&sessionOptions);
    iree_runtime_session_t* sess = nullptr;
    status = iree_runtime_session_create_with_device(instPtr.get(), &sessionOptions, dev, iree_runtime_instance_host_allocator(instPtr.get()), &sess);
    /// Session takes a reference to the device, so we release our reference
    iree_hal_device_release(dev);
    NES_DEBUG("Created IREE session")

    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Setup failed. Could not create Session");
    }
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> sessPtr(sess);

    status = iree_runtime_session_append_bytecode_module_from_memory(sessPtr.get(), compiledModel, iree_allocator_null());
    if (!iree_status_is_ok(status))
    {
        throw InferenceRuntime("Model Setup failed. Could not Load model");
    }

    NES_DEBUG("Read the model from the bytecode buffer");
    this->instance = std::move(instPtr);
    this->session = std::move(sessPtr);
}

void IREERuntimeWrapper::execute(const std::string& functionName, std::span<const std::byte> input, std::span<std::byte> output)
{
    iree_runtime_call_t call;
    if (this->function.module == nullptr)
    {
        auto* status = iree_runtime_call_initialize_by_name(session.get(), iree_make_cstring_view(functionName.c_str()), &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize function call");
        this->function = call.function;
    }
    else
    {
        auto* status = iree_runtime_call_initialize(session.get(), function, &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize cached function call");
    }
    /// Ensure call resources are released on all exit paths (including exceptions)
    SCOPE_EXIT
    {
        iree_runtime_call_deinitialize(&call);
    };

    iree_hal_device_t* dev = iree_runtime_session_device(session.get());
    iree_hal_allocator_t* deviceAllocator = iree_runtime_session_device_allocator(session.get());
    iree_status_t status = iree_ok_status();

    iree_hal_buffer_view_t* view = nullptr;
    status = iree_hal_buffer_view_allocate_buffer_copy(
        dev,
        deviceAllocator,
        this->nDim,
        this->inputShape.data(),
        IREE_HAL_ELEMENT_TYPE_FLOAT_32,
        IREE_HAL_ENCODING_TYPE_DENSE_ROW_MAJOR,
        iree_hal_buffer_params_t{
            .usage = IREE_HAL_BUFFER_USAGE_DEFAULT,
            .access = IREE_HAL_MEMORY_ACCESS_ALL,
            .type = IREE_HAL_MEMORY_TYPE_DEVICE_LOCAL,
            .queue_affinity = 0,
            .min_alignment = 0},
        iree_make_const_byte_span(input.data(), input.size()),
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
    if (outputSizeBytes > output.size())
    {
        throw InferenceRuntime("Model Execution failed. Model output size exceeds buffer capacity");
    }
    status = iree_hal_device_transfer_d2h(
        iree_runtime_session_device(session.get()),
        iree_hal_buffer_view_buffer(outputView),
        0,
        output.data(),
        outputSizeBytes,
        IREE_HAL_TRANSFER_BUFFER_FLAG_DEFAULT,
        iree_infinite_timeout());

    ireeCheckStatus(status, "Model Execution failed. Could not copy output tensor");
}

void IREERuntimeWrapper::setInputShape(std::vector<size_t> shape)
{
    this->inputShape = std::move(shape);
}

void IREERuntimeWrapper::setNDim(size_t numDims)
{
    this->nDim = numDims;
}

}
