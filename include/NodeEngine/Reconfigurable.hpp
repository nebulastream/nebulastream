/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_RECONFIGURABLE_HPP_
