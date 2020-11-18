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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_
#include <Windowing/JoinForwardRefs.hpp>

namespace NES::Join {

enum JoinActionType { LazyNestedLoopJoin };

class BaseJoinActionDescriptor {
  public:
    /**
     * @brief this function will return the type of the action
     * @return
     */
    virtual JoinActionType getActionType() = 0;

  protected:
    BaseJoinActionDescriptor(JoinActionType action);
    JoinActionType action;
};

}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_JOINACTIONDESCRIPTOR_HPP_
