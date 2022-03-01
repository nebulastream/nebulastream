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
#ifndef NES_NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYSTATUS_HPP_
#define NES_NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYSTATUS_HPP_
#include <string>
namespace NES {

/**
 * @brief Represents various states the user query goes through.
 *
 * Registered: Query is registered to be scheduled to the worker nodes (added to the queue).
 * Scheduling: Coordinator node is processing the Query and will transmit the execution pipelines to worker nodes.
 * Running: Query is now running successfully.
 * MarkedForStop: A request arrived into the system for stopping a query and system marks the query for stopping (added to the queue).
 * Stopped: Query was explicitly stopped either by system or by user.
 * Failed: Query failed because of some reason.
 */
enum QueryStatus { Registered, Scheduling, Running, MarkedForStop, Stopped, Failed, Restarting, Migrating };

std::string toString(const QueryStatus queryStatus);
QueryStatus stringToQueryStatusMap(std::string queryStatus);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYSTATUS_HPP_
