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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLERSLICES_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLERSLICES_HPP_

#include <Util/VirtualEnableSharedFromThis.hpp>

namespace NES::Runtime::Execution {

/**
 * @brief Interface to handle deleting queries from shared operators that contain slices.
 * @note The idea behind this interface was to have a interface that lives in nes-runtime and which allows the executionContext to access a operatorHandler
 */
class OperatorHandlerSlices {

  public:
    OperatorHandlerSlices() = default;

    /**
     * Adds a query with a given id at a given time to a (then) shared join. This means all slices after this deployment would have had
     * different start and stop times with which the tuples inside this slice might also be invalid.
     * This approach (Task #5114) addresses this by adding every slice to windows that could have tuples laying in this slice. On
     * the other hand new tuples get put into newly created slices with the correct start and end time. Once an old slice is probed
     * for a window tuples with a wrong ts can appear, so the probe does an extra check to ignore these tuples
     * @param queryId
     * @param deploymentTime the time at which this query is deployed
     */
    virtual void addQueryToSharedJoinApproachProbing(QueryId queryId, uint64_t deploymentTime) = 0;

    /**
     * Adds a query with a given id at a given time to a (then) shared join. This means all slices after this deployment would have had
     * different start and stop times with which the tuples inside this slice might also be invalid.
     * This approach (Task #5115) addresses this by changing the start and stop time of every slice that could be different after
     * the addition of the new query. For each changed slice the windows that it belongs to need to be updated and the records inside
     * the slice need to be handled. For this purpose records that don't fit in the new slice definition get deleted from the slice's
     * PagedVectorVarSized and get written to the actual slice that it now belongs to (new slices will be created if necessary)
     * @param queryId
     * @param deploymentTime the time at which this query is deployed
     */
    virtual void addQueryToSharedJoinApproachDeleting(QueryId queryId, uint64_t deploymentTime) = 0;

    /**
     * Removes a query from a shared join. As this query is stopping all tuples for this query need to be emitted. For this purpose
     * all current slices that are part of windows for this query get write locked and emitted with the tuples that have been written
     * to them. After applying the write lock, new tuples that would be part of this slice that are ingested instead open a new slice
     * that they get written to.
     * @param queryId
     * @param deploymentTime the time at which this query is deployed
     */
    virtual void removeQueryFromSharedJoin(QueryId queryId) = 0;

    //de-constructor needs NES_NOEXCEPT so operator handlers can inherit from this abstract class. (because another abstract class has the same)
    virtual ~OperatorHandlerSlices() NES_NOEXCEPT(false) = default;
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLERSLICES_HPP_
