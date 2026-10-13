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

#include <LookbackContext.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <LookbackConnections.hpp>
#include <LookbackProvider.hpp>
#include <LookbackTemplate.hpp>

namespace NES::Lookback
{

namespace
{

/// A UTF-8 continuation byte matches 0b10xxxxxx; every other byte starts a sequence.
constexpr unsigned char CONTINUATION_MASK = 0xC0U;
constexpr unsigned char CONTINUATION_MARKER = 0x80U;

/// Cuts `text` to at most `limit` bytes without splitting a UTF-8 sequence. The alert prose is
/// German — an umlaut severed mid-codepoint would render as a replacement character in the
/// clinician's message.
void truncateAtCodepointBoundary(std::string& text, size_t limit)
{
    if (text.size() <= limit)
    {
        return;
    }

    size_t end = limit;
    /// Walk back off any continuation byte to the sequence's lead byte.
    while (end > 0 && (static_cast<unsigned char>(text[end]) & CONTINUATION_MASK) == CONTINUATION_MARKER)
    {
        --end;
    }
    text.resize(end);
}

}

LookbackContext::LookbackContext(LookbackConnection connectionIn, std::string sqlIn, RowTemplate rowTemplateIn)
    : connection(std::move(connectionIn)), sql(std::move(sqlIn)), rowTemplate(std::move(rowTemplateIn))
{
}

LookbackContext::ThreadState& LookbackContext::threadState() const
{
    const auto id = std::this_thread::get_id();
    const std::scoped_lock lock(stateMutex);
    auto found = states.find(id);
    if (found == states.end())
    {
        found = states.emplace(id, std::make_unique<ThreadState>()).first;
    }
    return *found->second;
}

void LookbackContext::begin()
{
    threadState().parameters.clear();
}

void LookbackContext::bind(const int8_t* data, uint64_t size)
{
    threadState().parameters.emplace_back(
        reinterpret_cast<const char*>(data), size); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

uint32_t LookbackContext::run()
{
    auto& state = threadState();
    state.rendered.clear();

    /// The two failure modes are kept apart deliberately. A transport failure invalidates the
    /// connection, so the provider is dropped and the next alert reconnects. A rendering failure —
    /// the template and the query's column count disagreeing — is a permanent configuration error;
    /// dropping a healthy connection over it would mean a fresh ODBC connect on every alert.
    ResultRows rows;
    try
    {
        if (!state.provider)
        {
            state.provider = makeProvider(connection, sql);
        }
        rows = state.provider->run(state.parameters);
    }
    catch (const Exception& exception)
    {
        state.provider.reset();
        if (connection.limits.strict)
        {
            throw;
        }
        return failed(state, "failed", exception);
    }

    try
    {
        /// Both caps are applied here rather than only in the provider, so the rendering means the
        /// same thing whichever provider produced the rows.
        size_t rendered = 0;
        for (const auto& row : rows)
        {
            if (rendered >= connection.limits.maxRows)
            {
                NES_WARNING(
                    "Lookback '{}' returned more than max_rows={} row(s); the surplus was dropped",
                    connection.name,
                    connection.limits.maxRows);
                break;
            }
            rowTemplate.render(row, state.rendered);
            ++rendered;
            if (state.rendered.size() >= connection.limits.maxResultBytes)
            {
                NES_WARNING(
                    "Lookback '{}' rendering exceeded max_result_bytes={}; it was truncated",
                    connection.name,
                    connection.limits.maxResultBytes);
                break;
            }
        }
        truncateAtCodepointBoundary(state.rendered, connection.limits.maxResultBytes);
    }
    catch (const Exception& exception)
    {
        if (connection.limits.strict)
        {
            throw;
        }
        return failed(state, "could not be rendered", exception);
    }

    return static_cast<uint32_t>(state.rendered.size());
}

uint32_t LookbackContext::failed(ThreadState& state, const std::string_view what, const Exception& exception) const
{
    if (connection.onFailure)
    {
        NES_ERROR("Lookback '{}' {}; it renders as its failure marker: {}", connection.name, what, exception.what());
        state.rendered = *connection.onFailure;
    }
    else
    {
        NES_WARNING("Lookback '{}' {}; the alert is emitted without lookback context: {}", connection.name, what, exception.what());
        state.rendered.clear();
    }
    return static_cast<uint32_t>(state.rendered.size());
}

void LookbackContext::copyOut(int8_t* destination, uint32_t size) const
{
    const auto& rendered = threadState().rendered;
    INVARIANT(
        size == rendered.size(), "copyOut was asked for {} bytes but the last run() on this thread rendered {}", size, rendered.size());
    if (size != 0)
    {
        std::memcpy(destination, rendered.data(), size);
    }
}

void lookbackBeginProxy(LookbackContext* context)
{
    context->begin();
}

void lookbackBindProxy(LookbackContext* context, const int8_t* data, uint64_t size)
{
    context->bind(data, size);
}

uint32_t lookbackRunProxy(LookbackContext* context)
{
    return context->run();
}

void lookbackCopyProxy(LookbackContext* context, int8_t* destination, uint32_t size)
{
    context->copyOut(destination, size);
}

}
