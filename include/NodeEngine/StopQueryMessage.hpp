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

#ifndef NES_INCLUDE_NODEENGINE_STOPQUERYMESSAGE_H
#define NES_INCLUDE_NODEENGINE_STOPQUERYMESSAGE_H

#include <Network/NetworkMessage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <memory>
namespace NES::NodeEngine {
/**
* @brief Wrapper for UserData when
*/
class StopQueryMessage {
  public:
    /**
     * @param qep : Target QEP for reconfiguration
     * @param queryReconfigurationMessage :
     */
    StopQueryMessage(std::weak_ptr<Execution::ExecutableQueryPlan> qep,
                     Network::Messages::QueryReconfigurationMessage queryReconfigurationMessage);
    /**
     * @brief Returns QEP intended for reconfiguration
     * @return QEP intended for reconfiguration
     */
    std::weak_ptr<Execution::ExecutableQueryPlan> getTargetQep();
    /**
     * Get reconfiguration message that resulted in invoking stopping of query
     * @return QueryReconfigurationMessage
     */
    QueryReconfigurationPlanPtr getQueryReconfigurationPlan();

  private:
    std::weak_ptr<Execution::ExecutableQueryPlan> qep;
    QueryReconfigurationPlanPtr queryReconfigurationPlan;
};
typedef std::shared_ptr<StopQueryMessage> StopQueryMessagePtr;
}// namespace NES::NodeEngine
#endif//NES_INCLUDE_NODEENGINE_STOPQUERYMESSAGE_H
