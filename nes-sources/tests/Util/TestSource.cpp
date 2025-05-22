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

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <MemoryTestUtils.hpp>
#include <TestSource.hpp>

namespace
{
constexpr std::chrono::milliseconds DEFAULT_AWAIT_TIME = std::chrono::milliseconds(10000);
constexpr std::chrono::milliseconds IMMEDIATELY = std::chrono::milliseconds(0);
constexpr size_t DEFAULT_NUMBER_OF_LOCAL_BUFFERS = 4;
}

template <typename QueueType, typename Args>
bool tryIngestionUntil(QueueType& queue, Args&& args, std::function<bool()> condition)
{
    constexpr auto attempts = 10;
    for (size_t i = 0; i < attempts; ++i)
    {
        if (condition())
        {
            return true;
        }
        if (queue.tryWriteUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), args))
        {
            return true;
        }
    }
    NES_WARNING("Failed to inject data after {} attempts", attempts);
    return false;
}

bool NES::Sources::TestSourceControl::injectEoS()
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    if (tryIngestionUntil(queue, EoS{}, [this] { return wasClosed(); }))
    {
        return true;
    }
    throw TestException("Sources::TestSourceControl::injectEoS failed, maybe source has already been stopped");
}

bool NES::Sources::TestSourceControl::injectData(std::vector<std::byte> data, size_t numberOfTuples)
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    return tryIngestionUntil(queue, Data{.data = std::move(data), .numberOfTuples = numberOfTuples}, [this] { return wasClosed(); });
}

bool NES::Sources::TestSourceControl::injectError(std::string error)
{
    failed = true;
    if (tryIngestionUntil(queue, Error{std::move(error)}, [this] { return wasClosed(); }))
    {
        return true;
    }
    throw TestException("Sources::TestSourceControl::injectError failed, maybe source has already been stopped");
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

testing::AssertionResult NES::Sources::TestSourceControl::waitUntilOpened()
{
    return assertFutureStatus(this->openFuture.wait_for(DEFAULT_AWAIT_TIME));
}

testing::AssertionResult NES::Sources::TestSourceControl::waitUntilClosed()
{
    return assertFutureStatus(this->closeFuture.wait_for(DEFAULT_AWAIT_TIME));
}

testing::AssertionResult NES::Sources::TestSourceControl::waitUntilDestroyed()
{
    return assertFutureStatus(this->destroyedFuture.wait_for(DEFAULT_AWAIT_TIME));
}

bool NES::Sources::TestSourceControl::wasClosed() const
{
    return assertFutureStatus(closeFuture.wait_for(IMMEDIATELY));
}

bool NES::Sources::TestSourceControl::wasOpened() const
{
    return assertFutureStatus(openFuture.wait_for(IMMEDIATELY));
}

bool NES::Sources::TestSourceControl::wasDestroyed() const
{
    return assertFutureStatus(destroyedFuture.wait_for(IMMEDIATELY));
}

void NES::Sources::TestSourceControl::failDuringOpen(std::chrono::milliseconds blockFor)
{
    assert(!wasOpened() && "open was already called. failedDuringOpen should be called during the test setup not during runtime");
    fail_during_open_duration = blockFor;
    fail_during_open = true;
}

void NES::Sources::TestSourceControl::failDuringClose(std::chrono::milliseconds blockFor)
{
    assert(!wasOpened() && "open was already called. failedDuringClose should be called during the test setup not during runtime");
    fail_during_close_duration = blockFor;
    fail_during_close = true;
}

size_t NES::Sources::TestSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
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
        return 0;
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
        return 0;
    }
    INVARIANT(data->data.size() <= tupleBuffer.getBufferSize(), "Test source attempted to send a buffer which is to big");
    tupleBuffer.setNumberOfTuples(data->numberOfTuples);
    std::ranges::copy(data->data, tupleBuffer.getBuffer<std::byte>());
    return data->data.size();
}

void NES::Sources::TestSource::open()
{
    control->open.set_value();
    if (control->fail_during_open)
    {
        std::this_thread::sleep_for(control->fail_during_open_duration.load());
        throw TestException("I should throw here");
    }
}

void NES::Sources::TestSource::close()
{
    control->close.set_value();
    if (control->fail_during_close)
    {
        std::this_thread::sleep_for(control->fail_during_close_duration.load());
        throw TestException("I should throw here");
    }
}

std::ostream& NES::Sources::TestSource::toString(std::ostream& str) const
{
    return str << "Test Source";
}

NES::Sources::TestSource::TestSource(OriginId sourceId, const std::shared_ptr<TestSourceControl>& control)
    : sourceId(sourceId), control(control)
{
}

NES::Sources::TestSource::~TestSource()
{
    control->destroyed.set_value();
}

std::pair<std::unique_ptr<NES::Sources::SourceHandle>, std::shared_ptr<NES::Sources::TestSourceControl>>
NES::Sources::getTestSource(OriginId originId, std::shared_ptr<Memory::AbstractPoolProvider> bufferPool)
{
    auto ctrl = std::make_shared<TestSourceControl>();
    auto testSource = std::make_unique<TestSource>(originId, ctrl);
    auto sourceHandle = std::make_unique<SourceHandle>(
        std::move(originId), std::move(bufferPool), DEFAULT_NUMBER_OF_LOCAL_BUFFERS, std::move(testSource));
    return {std::move(sourceHandle), ctrl};
}
