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

namespace NES
{

struct WorkerStatus
{
    struct ActiveQuery
    {
        QueryId queryId = INVALID<QueryId>;
        std::chrono::system_clock::time_point started{};
    };

    struct TerminatedQuery
    {
        QueryId queryId = INVALID<QueryId>;
        std::chrono::system_clock::time_point started{};
        std::chrono::system_clock::time_point terminated{};
        std::optional<Exception> error{};
    };

    /// Currently we will not store all historical data on the WorkerNode.
    /// This timestamp indicates which events are captured by the WorkerStatus
    std::chrono::system_clock::time_point after{};
    std::vector<ActiveQuery> activeQueries{};
    std::vector<TerminatedQuery> terminatedQueries{};
};

}
