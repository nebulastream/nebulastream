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

#include <LocalQuery.hpp>

namespace NES
{


bool LocalQuery::couldBeRunning()
{
    auto monitor = monitoringState.rlock();
    if (monitor->missing)
    {
        return false;
    }
    if (!monitor->latestSnapshot)
    {
        return true;
    } else
    {
        return !(monitor->latestSnapshot->state == QueryStatus::Stopped || monitor->latestSnapshot->state == QueryStatus::Failed);
    }
}

void LocalQuery::setFailureInjectionFlag()
{
    auto monitor = monitoringState.wlock();
    monitor->injectFailure = true;

}

bool LocalQuery::testFailureInjectionFlag() {
    auto monitor = monitoringState.wlock();
    if (monitor->injectFailure)
    {
        monitor->injectFailure = false;
        return true;
    }
    return false;
}

bool LocalQuery::isMissing() const
{
    auto monitor = monitoringState.rlock();
    return monitor->missing;
}



bool LocalQuery::hasFailed()
{
    auto monitor = monitoringState.rlock();
    if (monitor->latestSnapshot && monitor->latestSnapshot.value().state == QueryStatus::Failed)
    {
        return true;
    }
    return false;
}

bool LocalQuery::hasStopped()
{
    auto monitor = monitoringState.rlock();
    if (monitor->latestSnapshot && monitor->latestSnapshot.value().state == QueryStatus::Stopped)
    {
        return true;
    }
    return false;
}

void LocalQuery::reset()
{
    auto monitor = monitoringState.wlock();
    monitor->latestSnapshot = std::nullopt;
    monitor->missing = false;
}

void LocalQuery::setNewQueryId(QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    monitor->queryId = queryId;
}

void LocalQuery::updateSnapshot(LocalQueryStatusSnapshot& snapshot, QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    if (queryId == monitor->queryId) /// snapshots of outdated epochs are discarded
    {
        monitor->latestSnapshot = snapshot;
    }
}

void LocalQuery::markMissing(QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    if (queryId == monitor->queryId) /// errors for outdated epochs are discarded
    {
        monitor->missing = true;
    }
}

void LocalQuery::heartbeat(bool successful)
{
    auto monitor = monitoringState.wlock();
    monitor->lastHeartbeatSuccessful = successful;
    if (successful)
    {
        monitor->lastHeartbeat = std::chrono::system_clock::now();
    }
}

std::expected<LocalQueryStatusSnapshot, Exception> LocalQuery::createProvisionalStatusSnapshot(bool completed) const
{
    LocalQueryStatusSnapshot returnValue;
    auto monitor = monitoringState.rlock();
    returnValue.queryId = monitor->queryId;
    if (monitor->latestSnapshot)
    {
        returnValue.metrics = monitor->latestSnapshot->metrics;
    }

    if (!completed)
    {
        returnValue.state = QueryStatus::Running;
    }
    else
    {
        returnValue.state = monitor->latestSnapshot->state;
    }

    return returnValue;
}

}