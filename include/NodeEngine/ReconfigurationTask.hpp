#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_

#include <NodeEngine/ReconfigurationType.hpp>
#include <NodeEngine/Reconfigurable.hpp>
#include <memory>
#include <Util/ThreadBarrier.hpp>

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
    explicit ReconfigurationTask(const QuerySubPlanId parentPlanId, ReconfigurationType type, Reconfigurable* instance = nullptr) : parentPlanId(parentPlanId), type(type), instance(instance), barrier(nullptr) {
        refCnt.store(0);
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     */
    explicit ReconfigurationTask(const ReconfigurationTask& other, uint32_t numThreads) : ReconfigurationTask(other) {
        barrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads);
    }

    /**
     * @brief copy constructor
     * @param that
     */
    ReconfigurationTask(const ReconfigurationTask& that) : parentPlanId(that.parentPlanId), type(that.type), instance(that.instance), barrier(nullptr) {
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
        barrier->wait();
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

  private:
    /**
     * @brief resouce cleanup method
     */
    void destroy() {
        barrier.reset();
    }

  private:
    /// type of the reconfiguration
    ReconfigurationType type;

    /// pointer to reconfigurable instance
    Reconfigurable* instance;

    /// pointer to thread barrier
    ThreadBarrierPtr barrier;

    /// ref counter
    std::atomic<uint32_t> refCnt;

    /// owning plan id
    const QuerySubPlanId parentPlanId;
};
}
#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
