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

#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <utility>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <ErrorHandling.hpp>

namespace NES::ConnTest
{

/// Holds the connector's own exception (real code), captured before `SourceThread` wraps it
/// as `RunningRoutineFailure`. First write wins.
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

/// `Source` decorator that intercepts the connector's lifecycle calls to give the harness
/// two things the bare engine path can't:
///   1. the connector's real error code — captured (into `ConnectorErrorSlot`) before
///      `SourceThread` wraps every exception as `RunningRoutineFailure`.
///   2. a synchronous open()-completion signal — `open()` runs on the SourceThread and a
///      successful open emits no event, so `opened` is fulfilled when `inner->open()`
///      returns/throws, letting OPEN report success or the real error deterministically.
/// All else delegates to inner; `addsMetadata()` MUST mirror inner (SourceThread keys its
/// metadata stamping on it).
class InterceptorSource final : public NES::Source
{
public:
    InterceptorSource(
        std::unique_ptr<NES::Source> inner, std::shared_ptr<std::promise<void>> opened, std::shared_ptr<ConnectorErrorSlot> errorSlot)
        : inner(std::move(inner)), opened(std::move(opened)), errorSlot(std::move(errorSlot))
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
            /// signal OPEN, then let SourceThread's failure path run.
            opened->set_exception(std::current_exception());
            throw;
        }
        opened->set_value();
    }

    NES::Source::FillTupleBufferResult fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override
    {
        try
        {
            return inner->fillTupleBuffer(tupleBuffer, stopToken);
        }
        catch (const NES::Exception& ex)
        {
            /// capture the real code before SourceThread wraps it.
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
            /// capture the real code; never rethrow — close() runs in a SCOPE_EXIT (throw
            /// mid-unwind → terminate).
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
};

}
