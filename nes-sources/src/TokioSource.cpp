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

#include <TokioSource.hpp>

#include <Sources/SourceReturnType.hpp>
#include <absl/functional/any_invocable.h>
#include <nes-source-runtime-bindings/lib.h>
#include <rfl/json/Parser.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <AsyncSemaphore.hpp>
#include <DetachedTask.hpp>
#include <IORuntime.hpp>
#include <QueryId.hpp>
#include <SourceRegistry.hpp>

/// Serialize a DescriptorConfig::Config as flat {"key":"value",...} JSON
/// for the Rust FFI, which expects HashMap<String, String>.
static std::string configToFlatJson(const NES::DescriptorConfig::Config& config)
{
    std::unordered_map<std::string, std::string> flat;
    for (const auto& [key, value] : config)
    {
        flat[key] = std::get<std::string>(value);
    }
    return rfl::json::write(flat);
}

namespace NES
{

struct DataHandler final : DataEmitter
{
    OriginId originId;
    SourceReturnType::AsyncEmitFunction emitFunction;
    std::shared_ptr<AsyncSemaphore> semaphore = std::make_shared<AsyncSemaphore>(4);
    BackpressureListener backpressureListener;

    DataHandler(OriginId origin_id, SourceReturnType::AsyncEmitFunction emit_function, BackpressureListener backpressureListener)
        : originId(origin_id), emitFunction(std::move(emit_function)), backpressureListener(std::move(backpressureListener))
    {
    }

    AsyncFunctionResult onData(TupleBuffer buffer, AsyncCompletionToken token) override
    {
        auto emitTask = emitFunction(
            originId,
            NES::SourceReturnType::Data{
                .buffer = std::move(buffer),
                .onComplete = std::function([semaphore = this->semaphore]() mutable { semaphore->release(); })});

        [](AsyncCompletionToken tok,
           BackpressureListener backpressure,
           std::shared_ptr<AsyncSemaphore> semaphore,
           coro::task<void> emitTask) mutable -> DetachedTask
        {
            co_await semaphore->acquire();
            co_await backpressure.waitAsync();
            co_await emitTask;
            std::move(tok)(AsyncCompletionResult::Success);
        }(std::move(token), backpressureListener, semaphore, std::move(emitTask));
        return AsyncFunctionResult::CallbackRegistered;
    }

    AsyncFunctionResult onError(std::string errorMessage, AsyncCompletionToken token) override
    {
        auto emitTask = emitFunction(originId, NES::SourceReturnType::Error{CannotOpenSource("Source Failure: {}", errorMessage)});
        [](AsyncCompletionToken tok, coro::task<void> emitTask) mutable -> DetachedTask
        {
            co_await emitTask;
            std::move(tok)(AsyncCompletionResult::Success);
        }(std::move(token), std::move(emitTask));
        return AsyncFunctionResult::CallbackRegistered;
    }

    AsyncFunctionResult onEoS(AsyncCompletionToken token) override
    {
        auto emitTask = emitFunction(originId, NES::SourceReturnType::EoS{});
        [](AsyncCompletionToken tok, coro::task<void> emitTask) mutable -> DetachedTask
        {
            co_await emitTask;
            std::move(tok)(AsyncCompletionResult::Success);
        }(std::move(token), std::move(emitTask));
        return AsyncFunctionResult::CallbackRegistered;
    }
};

struct TokioSourceContext
{
    std::optional<size_t> stopEpoch;
    std::atomic_size_t epochCounter;
    std::shared_ptr<SourceContext> context;
    rust::Box<RustSourceHandle> handle;

    TokioSourceContext(
        const SourceDescriptor& descriptor,
        QueryId queryId,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        SourceReturnType::AsyncEmitFunction emitFunction,
        BackpressureListener backpressureListener,
        OriginId originId)
        : epochCounter(0)
        , context(std::make_shared<SourceContext>(
              SourceContext{.emitter = std::make_unique<DataHandler>(originId, std::move(emitFunction), std::move(backpressureListener))}))
        , handle(source_create_handle(
              QueryContext{
                  .query_id = queryId.getLocalQueryId().getRawValue(),
                  .distributed_query_id = queryId.getDistributedQueryId().getRawValue(),
                  .source_id = originId.getRawValue(),
                  .source_type = descriptor.getSourceType(),
                  .source_config = configToFlatJson(descriptor.getConfig())},
              context,
              std::move(bufferProvider)))
    {
    }
};
}

NES::TokioSource::TokioSource(
    BackpressureListener listener,
    SourceDescriptor descriptor,
    OriginId originId,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    SourceRuntimeConfiguration runtimeConfiguration)
    : context(nullptr)
    , originId(originId)
    , descriptor(descriptor)
    , backpressureHandler(listener)
    , bufferProvider(std::move(bufferProvider))
    , runtimeConfiguration(std::move(runtimeConfiguration))
{
}

NES::TokioSource::~TokioSource()
{
    INVARIANT(gracefulStop, "TokioSource was not stopped gracefully");
}

bool NES::TokioSource::start(
    QueryId queryId, NES::SourceReturnType::EmitFunction&&, NES::SourceReturnType::AsyncEmitFunction&& asyncEmitFunction)
{
    this->context = std::make_unique<TokioSourceContext>(
        descriptor, queryId, bufferProvider, std::move(asyncEmitFunction), std::move(backpressureHandler), originId);
    return true;
}

NES::OriginId NES::TokioSource::getSourceId() const
{
    return originId;
}

const NES::SourceRuntimeConfiguration& NES::TokioSource::getRuntimeConfiguration() const
{
    return runtimeConfiguration;
}

std::ostream& NES::TokioSource::toString(std::ostream& os) const
{
    return os << "TokioSource::toString()";
}

void NES::TokioSource::stop()
{
    std::ignore = tryStop(std::chrono::milliseconds(0));
}

NES::SourceReturnType::TryStopResult NES::TokioSource::tryStop(std::chrono::milliseconds)
{
    NES_DEBUG("Stopping tokio source");
    if (!context)
    {
        return SourceReturnType::TryStopResult::NOT_RUNNING;
    }

    if (::source_stop(*context->handle))
    {
        gracefulStop = true;
        context.reset();
        return SourceReturnType::TryStopResult::SUCCESS;
    }
    else
    {
        return SourceReturnType::TryStopResult::TIMEOUT;
    }
}
