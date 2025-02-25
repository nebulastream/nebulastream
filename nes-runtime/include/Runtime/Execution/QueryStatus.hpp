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
#include <ostream>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime::Execution
{
enum class QueryStatus : uint8_t
{
    Registered,
    Running, /// Deployed->Running when calling start()
    Stopped, /// Running->Stopped when calling stop() and in Running state
    Failed,
};

inline std::ostream& operator<<(std::ostream& ostream, const QueryStatus& status)
{
    return ostream << magic_enum::enum_name(status);
}

namespace QueryStatusUtil
{

inline bool hasRestarted(QueryStatus previousState, QueryStatus currentState)
{
    return (previousState == QueryStatus::Failed || previousState == QueryStatus::Stopped) && currentState == QueryStatus::Running;
}

inline bool isRegisteredButNotRunning(QueryStatus status)
{
    return status == QueryStatus::Registered || status == QueryStatus::Stopped || status == QueryStatus::Failed;
}
}
}

FMT_OSTREAM(NES::Runtime::Execution::QueryStatus);
