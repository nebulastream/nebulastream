//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELEXECUTIONPLAN_H
#define NES_UNIKERNELEXECUTIONPLAN_H

#include "Runtime/Execution/UnikernelPipelineExecutionContext.h"

namespace NES::Unikernel {
    template<typename Source, typename Sink, typename Stage, typename ...Stages>
    class UnikernelExecutionPlan {
        UnikernelPipelineExecutionContext<Sink, Stage, Stages...> firstContext;
        Source source = Source(&firstContext);

    public:
        void stop() {
            source.stop();
            firstContext.stop();
        }

        void execute() {
            source.start();
        }

        void setup() {
            source.setup();
            firstContext.setup();
        }
    };

}

#endif //NES_UNIKERNELEXECUTIONPLAN_H
