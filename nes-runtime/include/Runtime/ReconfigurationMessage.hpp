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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONMESSAGE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONMESSAGE_HPP_

#include <any>
#include <atomic>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/ReconfigurationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadBarrier.hpp>
namespace NES::Runtime
{

class Reconfigurable;
using ReconfigurablePtr = std::shared_ptr<Reconfigurable>;

/// A reconfiguration message kickstarts the configuration process that passes a reconfiguration task to every running thread.
/// A reconfiguration task is a synchronization/thread barrier. Upon receiving a reconfiguration task, a worker threads halts
/// execution, until all worker threads received the reconfiguration message.
class ReconfigurationMessage
{
    using ThreadBarrierPtr = std::unique_ptr<ThreadBarrier>;

public:
    explicit ReconfigurationMessage(
        const SharedQueryId sharedQueryId,
        const DecomposedQueryPlanId parentPlanId,
        ReconfigurationType type,
        ReconfigurablePtr instance = nullptr,
        std::any&& userdata = nullptr)
        : type(type)
        , instance(std::move(instance))
        , syncBarrier(nullptr)
        , postSyncBarrier(nullptr)
        , sharedQueryId(sharedQueryId)
        , parentPlanId(parentPlanId)
        , userdata(std::move(userdata))
    {
        refCnt.store(0);
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
    }

    explicit ReconfigurationMessage(
        const SharedQueryId sharedQueryId,
        const DecomposedQueryPlanId parentPlanId,
        ReconfigurationType type,
        uint64_t numThreads,
        ReconfigurablePtr instance,
        std::any&& userdata = nullptr,
        bool blocking = false)
        : type(type)
        , instance(std::move(instance))
        , postSyncBarrier(nullptr)
        , sharedQueryId(sharedQueryId)
        , parentPlanId(parentPlanId)
        , userdata(std::move(userdata))
    {
        NES_ASSERT(this->instance, "invalid instance");
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
        syncBarrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads + (blocking ? 1 : 0));
        if (blocking)
        {
            postSyncBarrier = std::make_unique<ThreadBarrier>(numThreads + 1);
        }
    }

    explicit ReconfigurationMessage(const ReconfigurationMessage& other, uint64_t numThreads, bool blocking = false)
        : ReconfigurationMessage(other)
    {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
        syncBarrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads + (blocking ? 1 : 0));
        if (blocking)
        {
            postSyncBarrier = std::make_unique<ThreadBarrier>(numThreads + 1);
        }
    }

    ReconfigurationMessage(const ReconfigurationMessage& other)
        : type(other.type)
        , instance(other.instance)
        , syncBarrier(nullptr)
        , postSyncBarrier(nullptr)
        , sharedQueryId(other.sharedQueryId)
        , parentPlanId(other.parentPlanId)
        , userdata(other.userdata)
    {
        /// nop
    }

    ~ReconfigurationMessage() { destroy(); }

    [[nodiscard]] ReconfigurationType getType() const { return type; }

    [[nodiscard]] SharedQueryId getQueryId() const { return sharedQueryId; }

    [[nodiscard]] DecomposedQueryPlanId getParentPlanId() const { return parentPlanId; }

    [[nodiscard]] ReconfigurablePtr getInstance() const { return instance; };

    /// issue a synchronization barrier for all threads
    void wait();

    void postReconfiguration();

    /// issue a synchronization barrier for all threads
    void postWait();

    /// Provides the userdata installed in this reconfiguration descriptor
    template <typename T>
    [[nodiscard]] T getUserData() const
    {
        NES_ASSERT2_FMT(userdata.has_value(), "invalid userdata");
        return std::any_cast<T>(userdata);
    }

private:
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
    const SharedQueryId sharedQueryId;

    /// owning plan id
    const DecomposedQueryPlanId parentPlanId;

    /// custom data
    std::any userdata;
};
} /// namespace NES::Runtime
#endif /// NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURATIONMESSAGE_HPP_
