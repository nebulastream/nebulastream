#ifndef NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
#define NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_

namespace NES {

class WorkerContext;
typedef WorkerContext& WorkerContextRef;

class Reconfigurable {
  public:
    virtual ~Reconfigurable() {
        // nop
    }

    virtual void reconfigure(WorkerContextRef) {
        // nop
    }

};

}


#endif//NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
