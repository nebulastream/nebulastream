#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_

#include <NodeEngine/ReconfigurationType.hpp>

namespace NES {

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

class ReconfigurationTask;

class Reconfigurable {
  public:
    virtual ~Reconfigurable() {
        // nop
    }

    virtual void reconfigure(ReconfigurationTask&, WorkerContextRef) {
        // nop
    }

    virtual void destroyCallback(ReconfigurationTask&) {
        // nop
    }
};

}


#endif//NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
