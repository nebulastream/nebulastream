#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_

#include <NodeEngine/ReconfigurationType.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <memory>
#include <Util/ThreadBarrier.hpp>

namespace NES {



class ReconfigurationTask {
  public:
    explicit ReconfigurationTask(const QuerySubPlanId parentPlanId, ReconfigurationType type, Reconfigurable* instance = nullptr) : parentPlanId(parentPlanId), type(type), instance(instance), barrier(nullptr) {
        refCnt.store(0);
    }

    explicit ReconfigurationTask(const ReconfigurationTask& other, uint32_t numThreads) : ReconfigurationTask(other) {
        barrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads);
    }

    ReconfigurationTask(const ReconfigurationTask& that) : parentPlanId(that.parentPlanId), type(that.type), instance(that.instance), barrier(nullptr) {
        // nop
    }

    ~ReconfigurationTask() {
        destroy();
    }

    ReconfigurationType getType() const {
        return type;
    }

    QuerySubPlanId getParentPlanId() const {
        return parentPlanId;
    }

    Reconfigurable* getInstance() const {
        return instance;
    };

    void wait() {
        barrier->wait();
    }

    void postReconfiguration() {
        if (refCnt.fetch_sub(1) == 1) {
            instance->destroyCallback(*this);
            destroy();
        }
    }

  private:
    void destroy() {
        barrier.reset();
    }

  private:
    /// type of the reconfiguration
    ReconfigurationType type;

    /// pointer to reconfigurable instance
    Reconfigurable* instance;

    /// pointer to thread barrier
    std::unique_ptr<ThreadBarrier> barrier;

    /// ref counter
    std::atomic<uint32_t> refCnt;

    /// owning plan id
    const QuerySubPlanId parentPlanId;
};
}
#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
