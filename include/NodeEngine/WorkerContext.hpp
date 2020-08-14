#ifndef NES_WORKERCONTEXT_HPP_
#define NES_WORKERCONTEXT_HPP_

#include <cstdint>

namespace NES {

/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext {

    uint32_t worker_id;

  public:
    explicit WorkerContext(uint32_t worker_id) : worker_id(worker_id) {
        // nop
    }

    uint32_t id() const {
        return worker_id;
    }
};
}// namespace NES
#endif//NES_WORKERCONTEXT_HPP_
