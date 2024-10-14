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
namespace NES::Runtime::Execution
{
enum class QueryStatus : uint8_t
{
    Registered,
    Unregistered, /// Instead of this state, in the future, we might want to remove queries completely from the system
    Deployed, /// Created->Deployed when calling setup()
    Running, /// Deployed->Running when calling start()
    Finished, /// Running->Finished when all data sources soft stop
    Stopped, /// Running->Stopped when calling stop() and in Running state
    Failed,
    Invalid
};

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
} /// namespace NES::Runtime::Execution
