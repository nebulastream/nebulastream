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

#include <InferenceRuntime.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <iree/runtime/api.h>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>
#include <scope_guard.hpp>

namespace NES
{

namespace
{

struct InstanceDeleter
{
    void operator()(iree_runtime_instance_t* ptr) const
    {
        if (ptr != nullptr)
        {
            iree_runtime_instance_release(ptr);
        }
    }
};

struct SessionDeleter
{
    void operator()(iree_runtime_session_t* ptr) const
    {
        if (ptr != nullptr)
        {
            iree_runtime_session_release(ptr);
        }
    }
};

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

/// Creates a local-sync session on the instance. The session takes its own device reference, so the device is released here; the
/// caller owns the returned session.
iree_runtime_session_t* createLocalSyncSession(iree_runtime_instance_t* instance)
{
    iree_hal_device_t* dev = nullptr;
    ireeCheckStatus(
        iree_runtime_instance_try_create_default_device(instance, iree_make_cstring_view("local-sync"), &dev),
        "Model Setup failed. Could not create IREE device");
    iree_runtime_session_options_t sessionOptions;
    iree_runtime_session_options_initialize(&sessionOptions);
    iree_runtime_session_t* sess = nullptr;
    const iree_status_t status
        = iree_runtime_session_create_with_device(instance, &sessionOptions, dev, iree_runtime_instance_host_allocator(instance), &sess);
    iree_hal_device_release(dev);
    ireeCheckStatus(status, "Model Setup failed. Could not create Session");
    return sess;
}

/// IREE registers several process-global tables lazily on first use: VM builtin types at instance creation, HAL types and CPU
/// feature detection at device/session creation. Those first-time registrations are not thread-safe, so when worker threads each
/// build their own IREE runtime concurrently they race (TSan: iree_vm_instance_register_type, iree_hal_*_type, iree_cpu_initialize).
/// Share one instance across all threads and warm up a session once here, single-threaded, so every global is registered before any
/// worker creates its own session; afterwards per-thread session creation only reads those tables. The Meyers singleton gives
/// thread-safe one-time init for free.
std::shared_ptr<iree_runtime_instance_t> sharedInstance()
{
    static const std::shared_ptr<iree_runtime_instance_t> instance = []
    {
        iree_runtime_instance_options_t options;
        iree_runtime_instance_options_initialize(&options);
        iree_runtime_instance_options_use_all_available_drivers(&options);
        iree_runtime_instance_t* inst = nullptr;
        ireeCheckStatus(
            iree_runtime_instance_create(&options, iree_allocator_system(), &inst),
            "Model Setup failed. Could not create IREE runtime instance");
        std::shared_ptr<iree_runtime_instance_t> ptr(inst, InstanceDeleter{});
        iree_runtime_session_release(createLocalSyncSession(ptr.get()));
        return ptr;
    }();
    return instance;
}

}

struct InferenceRuntime::Impl
{
    std::shared_ptr<iree_runtime_instance_t> instance;
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> session;
    iree_vm_function_t function{};
};

InferenceRuntime::InferenceRuntime() : impl(std::make_unique<Impl>())
{
}

InferenceRuntime::~InferenceRuntime() = default;
InferenceRuntime::InferenceRuntime(InferenceRuntime&&) noexcept = default;
InferenceRuntime& InferenceRuntime::operator=(InferenceRuntime&&) noexcept = default;

void InferenceRuntime::setup(const CompiledModel& model)
{
    /// One IREE instance shared by all workers (globals already registered, see sharedInstance); the session below is per-thread.
    std::shared_ptr<iree_runtime_instance_t> instPtr = sharedInstance();
    std::unique_ptr<iree_runtime_session_t, SessionDeleter> sessPtr{createLocalSyncSession(instPtr.get())};

    NES_DEBUG("Read the model from the bytecode buffer");
    const iree_const_byte_span_t byteCodeSpan{
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) IREE API requires const uint8_t* but bytecode is stored as std::byte
        .data = reinterpret_cast<const uint8_t*>(model.getData().data()),
        .data_length = model.getData().size()};
    const iree_status_t status
        = iree_runtime_session_append_bytecode_module_from_memory(sessPtr.get(), byteCodeSpan, iree_allocator_null());
    ireeCheckStatus(status, "Model Setup failed. Could not Load model");

    impl->instance = std::move(instPtr);
    impl->session = std::move(sessPtr);
    this->inputShape = model.getInputShape();
    this->nDim = model.getNDim();
    this->functionName = model.getFunctionName();

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->inputData = std::make_unique<std::byte[]>(model.inputSize());
    this->inputSize = model.inputSize();
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->outputData = std::make_unique<std::byte[]>(model.outputSize());
    this->outputSize = model.outputSize();
}

void InferenceRuntime::infer()
{
    iree_runtime_call_t call;
    if (impl->function.module == nullptr)
    {
        auto* status = iree_runtime_call_initialize_by_name(impl->session.get(), iree_make_cstring_view(functionName.c_str()), &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize function call");
        impl->function = call.function;
    }
    else
    {
        auto* status = iree_runtime_call_initialize(impl->session.get(), impl->function, &call);
        ireeCheckStatus(status, "Model Execution failed. Could not initialize cached function call");
    }
    SCOPE_EXIT
    {
        iree_runtime_call_deinitialize(&call);
    };

    iree_hal_device_t* dev = iree_runtime_session_device(impl->session.get());
    iree_hal_allocator_t* deviceAllocator = iree_runtime_session_device_allocator(impl->session.get());

    iree_hal_buffer_view_t* view = nullptr;
    iree_status_t status = iree_hal_buffer_view_allocate_buffer_copy(
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
        iree_make_const_byte_span(inputData.get(), inputSize),
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
    if (outputSizeBytes > outputSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Model output size {} B exceeds buffer capacity {} B", outputSizeBytes, outputSize);
    }
    status = iree_hal_device_transfer_d2h(
        iree_runtime_session_device(impl->session.get()),
        iree_hal_buffer_view_buffer(outputView),
        0,
        outputData.get(),
        outputSizeBytes,
        IREE_HAL_TRANSFER_BUFFER_FLAG_DEFAULT,
        iree_infinite_timeout());

    ireeCheckStatus(status, "Model Execution failed. Could not copy output tensor");
}

}
