/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_

#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/ReconfigurationType.hpp>
#include <Util/ThreadBarrier.hpp>
#include <memory>

namespace NES {

/**
 * @brief this class contains the description of the reconfiguration that
 * must be carried out
 */
class ReconfigurationTask {
    using ThreadBarrierPtr = std::unique_ptr<ThreadBarrier>;

  public:
    /**
     * @brief create a reconfiguration task that will be used to kickstart the reconfiguration process
     * @param parentPlanId the owning plan id
     * @param type what kind of reconfiguration we want
     * @param instance the target of the reconfiguration
     */
    explicit ReconfigurationTask(const QuerySubPlanId parentPlanId, ReconfigurationType type, Reconfigurable* instance = nullptr)
        : parentPlanId(parentPlanId), type(type), instance(instance), syncBarrier(nullptr), postSyncBarrier(nullptr) {
        refCnt.store(0);
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     */
    explicit ReconfigurationTask(const ReconfigurationTask& other, size_t numThreads, bool blocking = false) : ReconfigurationTask(other) {
        syncBarrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads + (blocking ? 1 : 0));
        if (blocking) {
            postSyncBarrier = std::make_unique<ThreadBarrier>(numThreads + 1);
        }
    }

    /**
     * @brief copy constructor
     * @param that
     */
    ReconfigurationTask(const ReconfigurationTask& that) : parentPlanId(that.parentPlanId), type(that.type), instance(that.instance), syncBarrier(nullptr), postSyncBarrier(nullptr) {
        // nop
    }

    ~ReconfigurationTask() {
        destroy();
    }

    /**
     * @brief get the reconfiguration type
     * @return the reconfiguration type
     */
    ReconfigurationType getType() const {
        return type;
    }

    /**
     * @brief get the target plan id
     * @return the plan id
     */
    QuerySubPlanId getParentPlanId() const {
        return parentPlanId;
    }

    /**
     * @brief get the target instance to reconfigura
     * @return the target instance
     */
    Reconfigurable* getInstance() const {
        return instance;
    };

    /**
     * @brief issue a synchronization barrier for all threads
     */
    void wait() {
        syncBarrier->wait();
    }

    /**
     * @brief callback executed after the reconfiguration is carried out
     */
    void postReconfiguration() {
        if (refCnt.fetch_sub(1) == 1) {
            instance->destroyCallback(*this);
            destroy();
        }
    }

    /**
     * @brief issue a synchronization barrier for all threads
     */
    void postWait() {
        if (postSyncBarrier != nullptr) {
            postSyncBarrier->wait();
        }
    }

  private:
    /**
     * @brief resouce cleanup method
     */
    void destroy() {
        syncBarrier.reset();
        postSyncBarrier.reset();
    }

  private:
    /// type of the reconfiguration
    ReconfigurationType type;

    /// pointer to reconfigurable instance
    Reconfigurable* instance;

    /// pointer to initial thread barrier
    ThreadBarrierPtr syncBarrier;

    /// pointer to last thread barrier
    ThreadBarrierPtr postSyncBarrier;

    /// ref counter
    std::atomic<uint32_t> refCnt;

    /// owning plan id
    const QuerySubPlanId parentPlanId;
};
}// namespace NES
#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
