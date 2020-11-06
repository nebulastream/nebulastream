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

#ifndef NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#define NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
#include <Windowing/WindowHandler/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {

class LogicalJoinDefinition {
  public:
    static LogicalJoinDefinitionPtr create(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue, Windowing::WindowTypePtr windowType);

    LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKey, FieldAccessExpressionNodePtr rightJoinKey, FieldAccessExpressionNodePtr leftJoinValue, FieldAccessExpressionNodePtr rightJoinValue, Windowing::WindowTypePtr windowType);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getLeftJoinKey();
    void setLeftJoinKey(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for on right join key
     */
    FieldAccessExpressionNodePtr getRightJoinKey();
    void setRightJoinKey(FieldAccessExpressionNodePtr key);

    /**
    * @brief getter/setter for on left join value
    */
    FieldAccessExpressionNodePtr getLeftJoinValue();
    void setLeftJoinValue(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for on right join key
     */
    FieldAccessExpressionNodePtr getRightJoinValue();
    void setRightJoinValue(FieldAccessExpressionNodePtr key);

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();
    void setWindowType(Windowing::WindowTypePtr windowType);

  private:
    FieldAccessExpressionNodePtr leftJoinKey;
    FieldAccessExpressionNodePtr rightJoinKey;
    FieldAccessExpressionNodePtr leftJoinValue;
    FieldAccessExpressionNodePtr rightJoinValue;

    Windowing::WindowTypePtr windowType;
};

typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace NES::Join
#endif//NES_INCLUDE_JOIN_LOGICALJOINDEFINITION_HPP_
