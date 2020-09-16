#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURATIONDESCRIPTOR_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURATIONDESCRIPTOR_HPP_

#include <NodeEngine/Reconfigurable.hpp>
#include <memory>
#include <Util/ThreadBarrier.hpp>

namespace NES {

enum ReconfigurationType : uint8_t {
    Initialize,
};

class ReconfigurationDescriptor {
  public:
    explicit ReconfigurationDescriptor(ReconfigurationType type, Reconfigurable* instance = nullptr) : type(type), instance(instance), barrier(nullptr) {
        // nop
    }

    explicit ReconfigurationDescriptor(const ReconfigurationDescriptor& other, uint32_t numThreads) : ReconfigurationDescriptor(other) {
        barrier = std::make_unique<ThreadBarrier>(numThreads);
    }

    ReconfigurationDescriptor(const ReconfigurationDescriptor& that) : type(that.type), instance(that.instance), barrier(nullptr) {
        // nop
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

  private:
    /// type of the reconfiguration
    ReconfigurationType type;

    /// pointer to reconfigurable instance
    Reconfigurable* instance;

    /// pointer to thread barrier
    std::unique_ptr<ThreadBarrier> barrier;
};
}
#endif//NES_INCLUDE_NODEENGINE_RECONFIGURATIONDESCRIPTOR_HPP_
