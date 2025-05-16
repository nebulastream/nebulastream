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

#include <chrono>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
/// The QueryEngine is fundamentally multithreaded, and events occur asynchronously in its worker threads.
/// This has the unfortunate side effect of potentially reporting events out of order.
/// For example, the thread reporting the `Stopped` QueryStatus might overtake the thread reporting the `Running` QueryStatus.
struct AbstractQueryStatusListener
{
    virtual ~AbstractQueryStatusListener() noexcept = default;

    virtual bool logSourceTermination(QueryId queryId, OriginId sourceId, QueryTerminationType, std::chrono::system_clock::time_point) = 0;

    virtual bool logQueryFailure(QueryId queryId, Exception exception, std::chrono::system_clock::time_point) = 0;

    virtual bool logQueryStatusChange(QueryId queryId, QueryStatus status, std::chrono::system_clock::time_point) = 0;
};
}
