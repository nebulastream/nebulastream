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

#include <TestSource.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <semaphore>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Logger.hpp>
#include <SourceThreadHandle.hpp>
#include <Util/Overloaded.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <MemoryTestUtils.hpp>

#include "QueryId.hpp"

namespace
{
constexpr std::chrono::milliseconds DEFAULT_AWAIT_TIME = std::chrono::milliseconds(10000);
constexpr std::chrono::milliseconds IMMEDIATELY = std::chrono::milliseconds(0);
constexpr size_t DEFAULT_NUMBER_OF_LOCAL_BUFFERS = 4;

template <typename QueueType, typename Args>
bool tryIngestionUntil(QueueType& queue, Args&& args, const std::function<bool()>& condition)
{
    constexpr auto attempts = 100;
    for (size_t i = 0; i < attempts; ++i)
    {
        if (condition())
        {
            return true;
        }
        if (queue.tryWriteUntil(
                std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(NES::TestSourceControl::RETRY_MULTIPLIER_MS * i),
                args))
        {
            return true;
        }
    }
    NES_WARNING("Failed to inject data after {} attempts", attempts);
    return false;
}
}

bool NES::TestSourceControl::injectEoS()
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    if (tryIngestionUntil(queue, EoS{}, [this] { return wasClosed(); }))
    {
        return true;
    }
    throw TestException("TestSourceControl::injectEoS failed, maybe source has already been stopped");
}

bool NES::TestSourceControl::injectData(std::vector<std::byte> data, size_t numberOfTuples)
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    return tryIngestionUntil(queue, Data{.data = std::move(data), .numberOfTuples = numberOfTuples}, [this] { return wasClosed(); });
}

bool NES::TestSourceControl::injectError(std::string error)
{
    failed = true;
    if (tryIngestionUntil(queue, Error{std::move(error)}, [this] { return wasClosed(); }))
    {
        return true;
    }
    throw TestException("TestSourceControl::injectError failed, maybe source has already been stopped");
}

testing::AssertionResult assertFutureStatus(std::future_status status)
{
    switch (status)
    {
        case std::future_status::ready:
            return testing::AssertionSuccess();
        case std::future_status::timeout:
        case std::future_status::deferred:
            return testing::AssertionFailure();
    }
}

testing::AssertionResult NES::TestSourceControl::waitUntilOpened()
{
    return assertFutureStatus(this->openFuture.wait_for(DEFAULT_AWAIT_TIME));
}

testing::AssertionResult NES::TestSourceControl::waitUntilClosed()
{
    return assertFutureStatus(this->closeFuture.wait_for(DEFAULT_AWAIT_TIME));
}

testing::AssertionResult NES::TestSourceControl::waitUntilDestroyed()
{
    return assertFutureStatus(this->destroyedFuture.wait_for(DEFAULT_AWAIT_TIME));
}

bool NES::TestSourceControl::wasClosed() const
{
    return assertFutureStatus(closeFuture.wait_for(IMMEDIATELY));
}

bool NES::TestSourceControl::wasOpened() const
{
    return assertFutureStatus(openFuture.wait_for(IMMEDIATELY));
}

bool NES::TestSourceControl::wasDestroyed() const
{
    return assertFutureStatus(destroyedFuture.wait_for(IMMEDIATELY));
}

void NES::TestSourceControl::failDuringOpen(std::chrono::milliseconds blockFor)
{
    assert(!wasOpened() && "open was already called. failedDuringOpen should be called during the test setup not during runtime");
    fail_during_open_duration = blockFor;
    fail_during_open = true;
}

void NES::TestSourceControl::failDuringClose(std::chrono::milliseconds blockFor)
{
    assert(!wasOpened() && "open was already called. failedDuringClose should be called during the test setup not during runtime");
    fail_during_close_duration = blockFor;
    fail_during_close = true;
}

NES::Source::FillTupleBufferResult NES::TestSource::fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    TestSourceControl::ControlData controlData;
    /// poll from the queue as long as stop was not requested.
    while (!stopToken.stop_requested()
           && !control->queue.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(1), controlData))
    {
    }

    if (stopToken.stop_requested())
    {
        NES_DEBUG("Test Source {} was requested to shutdown", this->sourceId);
        return FillTupleBufferResult::eos();
    }

    auto data = std::visit(
        Overloaded{
            [](const TestSourceControl::Error& error) -> std::optional<TestSourceControl::Data>
            {
                NES_DEBUG("Test Source is injecting error");
                throw TestException(error.error);
            },
            [](TestSourceControl::Data data)
            {
                NES_DEBUG("Test Source is injecting data");
                return std::optional(data);
            },
            [](TestSourceControl::EoS) -> std::optional<TestSourceControl::Data>
            {
                NES_DEBUG("Test Source is injecting end of stream");
                return std::nullopt;
            }},
        controlData);

    if (!data)
    {
        return FillTupleBufferResult::eos();
    }
    INVARIANT(data->data.size() <= tupleBuffer.getBufferSize(), "Test source attempted to send a buffer which is to big");
    tupleBuffer.setNumberOfTuples(data->numberOfTuples);
    std::ranges::copy(data->data, tupleBuffer.getAvailableMemoryArea().data());
    return FillTupleBufferResult::withBytes(data->data.size());
}

void NES::TestSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    control->open.set_value();
    if (control->fail_during_open)
    {
        std::this_thread::sleep_for(control->fail_during_open_duration.load());
        throw TestException("I should throw here");
    }
}

void NES::TestSource::close()
{
    control->close.set_value();
    if (control->fail_during_close)
    {
        std::this_thread::sleep_for(control->fail_during_close_duration.load());
        throw TestException("I should throw here");
    }
}

std::ostream& NES::TestSource::toString(std::ostream& str) const
{
    return str << "Test Source";
}

NES::TestSource::TestSource(OriginId sourceId, const std::shared_ptr<TestSourceControl>& control) : sourceId(sourceId), control(control)
{
}

NES::TestSource::~TestSource()
{
    control->destroyed.set_value();
}

std::pair<std::unique_ptr<NES::SourceHandle>, std::shared_ptr<NES::TestSourceControl>>
NES::getTestSource(BackpressureListener backpressureListener, OriginId originId, std::shared_ptr<AbstractBufferProvider> bufferPool)
{
    auto ctrl = std::make_shared<TestSourceControl>();
    auto testSource = std::make_unique<TestSource>(originId, ctrl);
    SourceRuntimeConfiguration runtimeConfig{DEFAULT_NUMBER_OF_LOCAL_BUFFERS};

    auto sourceHandle = std::make_unique<SourceThreadHandle>(
        std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), std::move(bufferPool), std::move(testSource));
    return {std::move(sourceHandle), ctrl};
}

/// A SourceHandle implementation for testing that exercises the async emit (emitWorkAsync) path.
/// It runs its own thread that reads from TestSourceControl and calls the AsyncEmitFunction directly.
class AsyncTestSourceHandle final : public NES::SourceHandle
{
public:
    AsyncTestSourceHandle(OriginId originId, std::shared_ptr<TestSourceControl> control, std::shared_ptr<AbstractBufferProvider> bufferPool)
        : configuration{DEFAULT_NUMBER_OF_LOCAL_BUFFERS}, originId(originId), control(std::move(control)), bufferPool(std::move(bufferPool))
    {
    }

    ~AsyncTestSourceHandle() override
    {
        if (thread.joinable())
        {
            stopSource.request_stop();
            thread.join();
        }
    }

    bool start(
        NES::QueryId queryId, NES::SourceReturnType::EmitFunction&&, NES::SourceReturnType::AsyncEmitFunction&& asyncEmitFunction) override
    {
        asyncEmit = std::move(asyncEmitFunction);
        control->open.set_value();
        thread = std::jthread(
            [this](std::stop_token stopToken)
            {
                while (!stopToken.stop_requested())
                {
                    TestSourceControl::ControlData controlData;
                    if (!control->queue.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(1), controlData))
                    {
                        continue;
                    }

                    auto event = std::visit(
                        Overloaded{
                            [](const TestSourceControl::Error& error) -> std::optional<SourceReturnType::SourceReturnType>
                            { return SourceReturnType::Error{Exception(error.error, 0)}; },
                            [this](TestSourceControl::Data data) -> std::optional<SourceReturnType::SourceReturnType>
                            {
                                auto buffer = bufferPool->getBufferBlocking();
                                buffer.setNumberOfTuples(data.numberOfTuples);
                                std::ranges::copy(data.data, buffer.getAvailableMemoryArea().data());
                                return SourceReturnType::Data{.buffer = std::move(buffer), .onComplete = {}};
                            },
                            [](TestSourceControl::EoS) -> std::optional<SourceReturnType::SourceReturnType>
                            { return SourceReturnType::EoS{}; }},
                        controlData);

                    if (!event)
                    {
                        break;
                    }

                    auto done = std::make_shared<std::binary_semaphore>(0);
                    auto result = asyncEmit(originId, std::move(*event), [done](auto) { done->release(); });
                    if (result == SourceReturnType::AsyncEmitResult::CALLBACK_REGISTERED)
                    {
                        while (!done->try_acquire_for(std::chrono::milliseconds(100)))
                        {
                            if (stopToken.stop_requested())
                            {
                                break;
                            }
                        }
                    }

                    /// EoS and Error terminate the source
                    if (std::holds_alternative<SourceReturnType::EoS>(*event) || std::holds_alternative<SourceReturnType::Error>(*event))
                    {
                        break;
                    }
                }
                control->close.set_value();
            });
        return true;
    }

    void stop() override
    {
        if (thread.joinable())
        {
            stopSource.request_stop();
            thread.join();
        }
    }

    SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds) override
    {
        if (!thread.joinable())
        {
            return SourceReturnType::TryStopResult::NOT_RUNNING;
        }
        stopSource.request_stop();
        thread.join();
        return SourceReturnType::TryStopResult::SUCCESS;
    }

    OriginId getSourceId() const override { return originId; }
    const SourceRuntimeConfiguration& getRuntimeConfiguration() const override { return configuration; }

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "AsyncTestSource"; }

private:
    SourceRuntimeConfiguration configuration;
    OriginId originId;
    std::shared_ptr<TestSourceControl> control;
    std::shared_ptr<AbstractBufferProvider> bufferPool;
    SourceReturnType::AsyncEmitFunction asyncEmit;
    std::stop_source stopSource;
    std::jthread thread;
};

std::pair<std::unique_ptr<NES::SourceHandle>, std::shared_ptr<NES::TestSourceControl>>
NES::getAsyncTestSource(OriginId originId, std::shared_ptr<AbstractBufferProvider> bufferPool)
{
    auto ctrl = std::make_shared<TestSourceControl>();
    auto sourceHandle = std::make_unique<AsyncTestSourceHandle>(std::move(originId), ctrl, std::move(bufferPool));
    return {std::move(sourceHandle), ctrl};
}
