/*
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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEJOINACTIONDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEJOINACTIONDESCRIPTOR_HPP_
#include<Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>

namespace NES::Join {

enum class JoinActionType : uint8_t { LazyNestedLoopJoin };

class BaseJoinActionDescriptor {
  public:
    virtual ~BaseJoinActionDescriptor() noexcept = default;
    /**
     * @brief this function will return the type of the action
     * @return
     */
    virtual JoinActionType getActionType() = 0;

    /**
     * @brief Checks if the two are equal
     * @param other: BaseJoinActionDescriptor that we want to compare this to
     * @return Boolean
     */
    virtual bool equals(const BaseJoinActionDescriptor& other) const;

  protected:
    explicit BaseJoinActionDescriptor(JoinActionType action);
    JoinActionType action;
};

}// namespace NES::Join
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEJOINACTIONDESCRIPTOR_HPP_
