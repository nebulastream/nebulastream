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

#include "TestSource.hpp"
#include <chrono>
#include <string>
#include <vector>
#include "Runtime/TupleBuffer.hpp"


template <typename Q, typename Args>
bool tryIngestionUntil(Q& q, Args&& args, std::function<bool()> condition)
{
    for (size_t i = 0; i < 10; ++i)
    {
        if (condition())
        {
            return true;
        }
        if (q.tryWriteUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(10), std::forward<Args>(args)))
        {
            return true;
        }
    }
    return false;
}

bool NES::Sources::TestSourceControl::injectEoS()
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    return tryIngestionUntil(q, EoS{}, [this] { return wasClosed(); });
}
bool NES::Sources::TestSourceControl::injectData(std::vector<std::byte> data, size_t numberOfTuples)
{
    PRECONDITION(!failed, "Should not be called on a failed source");
    return tryIngestionUntil(q, Data{data, numberOfTuples}, [this] { return wasClosed(); });
}
bool NES::Sources::TestSourceControl::injectError(std::string error)
{
    failed = true;
    return tryIngestionUntil(q, Error{error}, [this] { return wasClosed(); });
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
    return assertFutureStatus(this->openFuture.wait_for(std::chrono::milliseconds(100)));
}

testing::AssertionResult NES::Sources::TestSourceControl::waitUntilClosed()
{
    return assertFutureStatus(this->closeFuture.wait_for(std::chrono::milliseconds(100)));
}

testing::AssertionResult NES::Sources::TestSourceControl::waitUntilDestroyed()
{
    return assertFutureStatus(this->destroyedFuture.wait_for(std::chrono::milliseconds(100)));
}

bool NES::Sources::TestSourceControl::wasClosed() const
{
    return assertFutureStatus(closeFuture.wait_for(std::chrono::seconds(0)));
}
bool NES::Sources::TestSourceControl::wasOpened() const
{
    return assertFutureStatus(openFuture.wait_for(std::chrono::seconds(0)));
}
bool NES::Sources::TestSourceControl::wasDestroyed() const
{
    return assertFutureStatus(destroyedFuture.wait_for(std::chrono::seconds(0)));
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
bool NES::Sources::TestSource::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider&, std::shared_ptr<Schema>, const std::stop_token& stopToken)
{
    TestSourceControl::control_data cd;
    /// poll from the queue as long as stop was not requested.
    while (!stopToken.stop_requested()
           && !control_->q.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(1), cd))
    {
    }

    if (stopToken.stop_requested())
    {
        NES_DEBUG("Test Source {} was requested to shutdown", this->sourceId);
        return false;
    }

    auto data = std::visit(
        overloaded{
            [](TestSourceControl::Error e) -> std::optional<TestSourceControl::Data>
            {
                NES_DEBUG("Test Source is injecting error");
                throw std::runtime_error(e.error);
            },
            [](TestSourceControl::Data d)
            {
                NES_DEBUG("Test Source is injecting data");
                return std::optional(d);
            },
            [](TestSourceControl::EoS) -> std::optional<TestSourceControl::Data>
            {
                NES_DEBUG("Test Source is injecting end of stream");
                return std::nullopt;
            }},
        cd);

    if (!data)
    {
        return false;
    }
    NES_ASSERT(data->data.size() <= tupleBuffer.getBufferSize(), "Test source attempted to send a buffer which is to big");
    tupleBuffer.setNumberOfTuples(data->numberOfTuples);
    std::copy(data->data.begin(), data->data.end(), tupleBuffer.getBuffer<std::byte>());
    return true;
}
void NES::Sources::TestSource::open()
{
    control_->open.set_value();
    if (control_->fail_during_open)
    {
        std::this_thread::sleep_for(control_->fail_during_open_duration.load());
        throw std::runtime_error("I should throw here");
    }
}
void NES::Sources::TestSource::close()
{
    control_->close.set_value();
    if (control_->fail_during_close)
    {
        std::this_thread::sleep_for(control_->fail_during_close_duration.load());
        throw std::runtime_error("I should throw here");
    }
}
std::ostream& NES::Sources::TestSource::toString(std::ostream& str) const
{
    return str << "Test Source";
}

NES::Sources::TestSource::TestSource(OriginId sourceId, const std::shared_ptr<TestSourceControl>& control)
    : sourceId(sourceId), control_(control)
{
}
NES::Sources::TestSource::~TestSource()
{
    control_->destroyed.set_value();
}
std::pair<std::unique_ptr<NES::Sources::SourceHandle>, std::shared_ptr<NES::Sources::TestSourceControl>>
NES::Sources::getTestSource(OriginId originId, std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool)
{
    auto ctrl = std::make_shared<TestSourceControl>();
    auto testSource = std::make_unique<TestSource>(originId, ctrl);
    auto sourceHandle
        = std::make_unique<SourceHandle>(std::move(originId), Schema::create(), std::move(bufferPool), 4, std::move(testSource));
    return {std::move(sourceHandle), ctrl};
}
