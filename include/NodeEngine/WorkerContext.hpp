#ifndef NES_WORKERCONTEXT_HPP_
#define NES_WORKERCONTEXT_HPP_

#include <cstdint>

namespace NES {

/**
 * @brief This class is work in progress and is not documented
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
