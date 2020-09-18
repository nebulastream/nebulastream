#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_

#include <NodeEngine/ReconfigurationType.hpp>

namespace NES {

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

class ReconfigurationTask;

/**
 * @brief Nes components that require to be reconfigured at runtime need to
 * inherit from this class. It provides a reconfigure callback that will be called
 * per thread and a destroyCallback that will be called on the last thread the executes
 * the reconfiguration.
 */
class Reconfigurable {
  public:
    virtual ~Reconfigurable() {
        // nop
    }

    /**
     * @brief reconfigure callback that will be called per thread
     */
    virtual void reconfigure(ReconfigurationTask&, WorkerContextRef) {
        // nop
    }

    /**
     * @brief callback that will be called on the last thread the executes
     * the reconfiguration
     */
    virtual void destroyCallback(ReconfigurationTask&) {
        // nop
    }
};

}


#endif//NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
