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

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <utility>
#include <vector>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <ErrorHandling.hpp>

namespace NES::ConnTest
{

/// The connector's first exception, recorded before the source thread replaces its code with a generic failure.
struct ConnectorErrorSlot
{
    std::mutex mtx;
    std::optional<NES::Exception> error;

    void set(NES::Exception exception)
    {
        const std::scoped_lock lock(mtx);
        if (!error)
        {
            error = std::move(exception);
        }
    }

    std::optional<NES::Exception> peek()
    {
        const std::scoped_lock lock(mtx);
        return error;
    }
};

/// How long each read that returned a partly filled buffer took, from entering fillTupleBuffer to its return.
/// A source holds a partly filled buffer back for its flush interval, so a correct source never reports less than that interval.
/// Nothing bounds it from above but the step timeout: a correct source can return late when its thread is not scheduled,
/// so any tighter upper bound would depend on the machine's load.
struct PartialReadLog
{
    std::mutex mtx;
    std::vector<std::chrono::milliseconds> durations;

    void add(const std::chrono::milliseconds duration)
    {
        const std::scoped_lock lock(mtx);
        durations.push_back(duration);
    }

    /// The durations since the previous call.
    std::vector<std::chrono::milliseconds> take()
    {
        const std::scoped_lock lock(mtx);
        return std::exchange(durations, {});
    }
};

/// Wraps a connector so that the harness sees its real error codes and learns when open() has finished and when a read has begun.
/// A successful open emits no event, so the promise is the only completion signal.
/// Metadata support must match the inner source, because the source thread's metadata handling depends on it.
class InterceptorSource final : public NES::Source
{
public:
    InterceptorSource(
        std::unique_ptr<NES::Source> inner,
        std::shared_ptr<std::promise<void>> opened,
        std::shared_ptr<ConnectorErrorSlot> errorSlot,
        std::shared_ptr<std::atomic<bool>> reading,
        std::shared_ptr<PartialReadLog> partialReads)
        : inner(std::move(inner))
        , opened(std::move(opened))
        , errorSlot(std::move(errorSlot))
        , reading(std::move(reading))
        , partialReads(std::move(partialReads))
    {
    }

    void open(std::shared_ptr<NES::AbstractBufferProvider> bufferProvider) override
    {
        try
        {
            inner->open(std::move(bufferProvider));
        }
        catch (...)
        {
            /// Rethrow so that the source thread's failure path still runs.
            opened->set_exception(std::current_exception());
            throw;
        }
        opened->set_value();
    }

    NES::Source::FillTupleBufferResult fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override
    {
        reading->store(true);
        /// Taken before the connector starts its own clock, so a correct flush interval can only measure longer, never shorter.
        const auto start = std::chrono::steady_clock::now();
        try
        {
            auto result = inner->fillTupleBuffer(tupleBuffer, stopToken);
            if (!result.isEoS() && result.getNumberOfBytes() > 0 && result.getNumberOfBytes() < tupleBuffer.getBufferSize())
            {
                partialReads->add(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start));
            }
            return result;
        }
        catch (const NES::Exception& ex)
        {
            errorSlot->set(ex);
            throw;
        }
    }

    void close() override
    {
        try
        {
            inner->close();
        }
        catch (const NES::Exception& ex)
        {
            /// No rethrow: close() runs in a scope-exit guard, where a throw during unwinding terminates the process.
            errorSlot->set(ex);
        }
    }

    [[nodiscard]] bool addsMetadata() const override { return inner->addsMetadata(); }

protected:
    std::ostream& toString(std::ostream& str) const override { return str << *inner; }

private:
    std::unique_ptr<NES::Source> inner;
    std::shared_ptr<std::promise<void>> opened;
    std::shared_ptr<ConnectorErrorSlot> errorSlot;
    std::shared_ptr<std::atomic<bool>> reading;
    std::shared_ptr<PartialReadLog> partialReads;
};

}
