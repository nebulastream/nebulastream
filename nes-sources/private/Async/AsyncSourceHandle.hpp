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

#pragma once


#include <future>
#include <memory>
#include <ostream>
#include <utility>

#include <Async/AsyncSourceRunner.hpp>
#include <Async/IOThread.hpp>
#include <Sources/AsyncSource.hpp>
// #include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/AtomicState.hpp>
#include <boost/asio/cancellation_signal.hpp>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

namespace NES
{

class AsyncSourceHandle final : public SourceHandle
{
public:
    explicit AsyncSourceHandle(
        BackpressureListener backpressureListener,
        OriginId originId, /// Todo #241: Rethink use of originId for sources, use new identifier for unique identification.
        SourceRuntimeConfiguration configuration,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        std::unique_ptr<AsyncSource> sourceImplementation);
    ~AsyncSourceHandle() override = default;

    AsyncSourceHandle(const AsyncSourceHandle&) = delete;
    AsyncSourceHandle& operator=(const AsyncSourceHandle&) = delete;
    AsyncSourceHandle(AsyncSourceHandle&&) = delete;
    AsyncSourceHandle& operator=(AsyncSourceHandle&&) = delete;

    bool start(SourceReturnType::EmitFunction&& emitFn) override;
    bool internalStop();
    void stop() override;
    SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct Initial
    {
        Initial() = delete;

        explicit Initial(std::unique_ptr<AsyncSource> source) : sourceContext(std::move(source)) { }

        std::unique_ptr<AsyncSource> sourceContext;
    };

    struct Running
    {
        Running() = delete;

        explicit Running(
            std::unique_ptr<AsyncSource> source,
            SourceReturnType::EmitFunction emitFn,
            OriginId sourceId,
            std::shared_ptr<AbstractBufferProvider> bufferProvider)
            : ioThread{LazySingleton<IOThread>::getOrCreate()}
            , runner{std::make_shared<AsyncSourceRunner>(std::move(source), std::move(emitFn))}
            , cancellationSignal{std::make_unique<asio::cancellation_signal>()}
            , terminationFuture{asio::co_spawn(
                  ioThread->ioContext(),
                  runner->runningRoutine(sourceId, std::move(bufferProvider)),
                  bind_cancellation_slot(cancellationSignal->slot(), asio::use_future))}
        {
        }

        std::shared_ptr<IOThread> ioThread;
        std::shared_ptr<AsyncSourceRunner> runner;
        std::unique_ptr<asio::cancellation_signal> cancellationSignal;
        std::future<void> terminationFuture;
    };

    struct Stopped
    {
    };

    using AsyncSourceState = AtomicState<Initial, Running, Stopped>;
    AsyncSourceState state;
    std::shared_ptr<AbstractBufferProvider> bufferPool;
};

}
