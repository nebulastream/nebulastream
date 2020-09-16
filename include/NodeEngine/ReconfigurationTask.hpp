#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_

#include <NodeEngine/Reconfigurable.hpp>
#include <memory>
#include <Util/ThreadBarrier.hpp>

namespace NES {

enum ReconfigurationType : uint8_t {
    Initialize,
};

class ReconfigurationTask {
  public:
    explicit ReconfigurationTask(ReconfigurationType type, Reconfigurable* instance = nullptr) : type(type), instance(instance), barrier(nullptr) {
        refCnt.store(0);
    }

    explicit ReconfigurationTask(const ReconfigurationTask& other, uint32_t numThreads) : ReconfigurationTask(other) {
        barrier = std::make_unique<ThreadBarrier>(numThreads);
        refCnt.store(numThreads);
    }

    ReconfigurationTask(const ReconfigurationTask& that) : type(that.type), instance(that.instance), barrier(nullptr) {
        // nop
    }

    ~ReconfigurationTask() {
        destroy();
    }

    ReconfigurationType getType() const {
        return type;
    }

    Reconfigurable* getInstance() const {
        return instance;
    };

    void wait() {
        barrier->wait();
    }

    void postReconfiguration() {
        if (refCnt.fetch_sub(1) == 1) {
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
};
}
#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONTASK_HPP_
