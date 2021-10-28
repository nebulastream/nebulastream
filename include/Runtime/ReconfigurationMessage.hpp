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

#ifndef NES_INCLUDE_RUNTIME_RECONFIGURATION_MESSAGE_HPP_
#define NES_INCLUDE_RUNTIME_RECONFIGURATION_MESSAGE_HPP_

#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/ReconfigurationType.hpp>
#include <Util/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
#include <any>
#include <atomic>
#include <memory>

namespace NES::Runtime {

class Reconfigurable;
using ReconfigurablePtr = std::shared_ptr<Reconfigurable>;

/**
 * @brief this class contains the description of the reconfiguration that
 * must be carried out
 */
class ReconfigurationMessage {
    using ThreadBarrierPtr = std::unique_ptr<ThreadBarrier>;

  public:
    /**
     * @brief create a reconfiguration task that will be used to kickstart the reconfiguration process
     * @param parentPlanId the owning plan id
     * @param type what kind of reconfiguration we want
     * @param instance the target of the reconfiguration
     * @param userdata extra information to use in this reconfiguration
     */
    explicit ReconfigurationMessage(const QuerySubPlanId parentPlanId,
                                    ReconfigurationType type,
                                    ReconfigurablePtr instance = nullptr,
                                    std::any&& userdata = nullptr)
        : type(type), instance(std::move(instance)), syncBarrier(nullptr), postSyncBarrier(nullptr), parentPlanId(parentPlanId),
          userdata(std::move(userdata)) {
        refCnt.store(0);
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     * @param instance the target of the reconfiguration
     * @param userdata extra information to use in this reconfiguration
     * @param blocking whether the reconfiguration must block for completion
     */
    explicit ReconfigurationMessage(const QuerySubPlanId parentPlanId,
                                    ReconfigurationType type,
                                    uint64_t numThreads,
                                    ReconfigurablePtr instance,
                                    std::any&& userdata = nullptr,
                                    bool blocking = false)
        : type(type), instance(std::move(instance)), postSyncBarrier(nullptr), parentPlanId(parentPlanId),
          userdata(std::move(userdata)) {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
        syncBarrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads + (blocking ? 1 : 0));
        if (blocking) {
            postSyncBarrier = std::make_unique<ThreadBarrier>(numThreads + 1);
        }
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     * @param blocking whether the reconfiguration must block for completion
     */
    explicit ReconfigurationMessage(const ReconfigurationMessage& other, uint64_t numThreads, bool blocking = false)
        : ReconfigurationMessage(other) {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
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
    ReconfigurationMessage(const ReconfigurationMessage& that)
        : type(that.type), instance(that.instance), syncBarrier(nullptr), postSyncBarrier(nullptr),
          parentPlanId(that.parentPlanId), userdata(that.userdata) {
        // nop
    }

    /**
     * @brief Destructor that calls destroy()
     */
    ~ReconfigurationMessage() { destroy(); }

    /**
     * @brief get the reconfiguration type
     * @return the reconfiguration type
     */
    [[nodiscard]] ReconfigurationType getType() const { return type; }

    /**
     * @brief get the target plan id
     * @return the plan id
     */
    [[nodiscard]] QuerySubPlanId getParentPlanId() const { return parentPlanId; }

    /**
     * @brief get the target instance to reconfigura
     * @return the target instance
     */
    [[nodiscard]] ReconfigurablePtr getInstance() const { return instance; };

    /**
     * @brief issue a synchronization barrier for all threads
     */
    void wait();

    /**
     * @brief callback executed after the reconfiguration is carried out
     */
    void postReconfiguration();

    /**
     * @brief issue a synchronization barrier for all threads
     */
    void postWait();

    /**
     * @brief Provides the userdata installed in this reconfiguration descriptor
     * @tparam T the type of the reconfiguration's userdata
     * @return the user data value or error if that is not set
     */
    template<typename T>
    [[nodiscard]] T getUserData() const {
        NES_ASSERT2_FMT(userdata.has_value(), "invalid userdata");
        return std::any_cast<T>(userdata);
    }

  private:
    /**
     * @brief resouce cleanup method
     */
    void destroy();

    /// type of the reconfiguration
    ReconfigurationType type;

    /// pointer to reconfigurable instance
    ReconfigurablePtr instance;

    /// initial thread barrier
    ThreadBarrierPtr syncBarrier;

    /// last thread barrier
    ThreadBarrierPtr postSyncBarrier;

    /// ref counter
    std::atomic<uint32_t> refCnt{};

    /// owning plan id
    const QuerySubPlanId parentPlanId;

    /// custom data
    std::any userdata;
};
}// namespace NES::Runtime
#endif// NES_INCLUDE_RUNTIME_RECONFIGURATION_MESSAGE_HPP_
