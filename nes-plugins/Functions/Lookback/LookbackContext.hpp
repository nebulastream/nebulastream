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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ErrorHandling.hpp>
#include <LookbackConnections.hpp>
#include <LookbackProvider.hpp>
#include <LookbackTemplate.hpp>

namespace NES::Lookback
{

/// The runtime half of one LOOKBACK call site: the resolved connection, the SQL, and the parsed
/// row template, plus one lazily created provider per worker thread.
///
/// Per-thread rather than one-connection-plus-lock because ODBC handles are not thread safe and
/// the traced code splits a call across several proxy invocations — a lock spanning them would
/// deadlock if any of them threw. The states are owned by the context, so they die with the query
/// rather than lingering in thread-local storage keyed by a recycled address.
class LookbackContext
{
public:
    LookbackContext(LookbackConnection connection, std::string sql, RowTemplate rowTemplate);

    LookbackContext(const LookbackContext&) = delete;
    LookbackContext& operator=(const LookbackContext&) = delete;
    LookbackContext(LookbackContext&&) = delete;
    LookbackContext& operator=(LookbackContext&&) = delete;
    ~LookbackContext() = default;

    /// Clears the calling thread's parameter buffer, readying it for this tuple's binds.
    void begin();

    /// Appends one parameter value, in the order the `?` markers appear in the SQL.
    void bind(const int8_t* data, uint64_t size);

    /// Runs the query and renders the rows. Returns the byte length of the rendering, which the
    /// caller uses to size the arena allocation before calling copyOut.
    ///
    /// On a failure with `strict` unset this logs and renders the connection's `on_failure` marker,
    /// or nothing when it has none — an alert that fires is clinically more important than the
    /// context attached to it.
    uint32_t run();

    /// Copies the rendering produced by the last run() on this thread into `destination`.
    void copyOut(int8_t* destination, uint32_t size) const;

    [[nodiscard]] const LookbackConnection& getConnection() const { return connection; }

private:
    struct ThreadState
    {
        std::unique_ptr<LookbackProvider> provider;
        std::vector<std::string> parameters;
        std::string rendered;
    };

    /// Stable reference: the states are heap-allocated, so the map may rehash while a caller holds
    /// one.
    ThreadState& threadState() const;

    /// What a failure renders as when `strict` is unset: the marker, or nothing.
    uint32_t failed(ThreadState& state, std::string_view what, const Exception& exception) const;

    LookbackConnection connection;
    std::string sql;
    RowTemplate rowTemplate;

    mutable std::mutex stateMutex;
    mutable std::unordered_map<std::thread::id, std::unique_ptr<ThreadState>> states;
};

/// Proxy entry points. `nautilus::invoke` binds a plain function pointer, so these cannot be member
/// functions.
void lookbackBeginProxy(LookbackContext* context);
void lookbackBindProxy(LookbackContext* context, const int8_t* data, uint64_t size);
uint32_t lookbackRunProxy(LookbackContext* context);
void lookbackCopyProxy(LookbackContext* context, int8_t* destination, uint32_t size);

}
