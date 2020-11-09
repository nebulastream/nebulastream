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

#ifndef NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_
#define NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class SerializableExpression;

/**
 * @brief The ExpressionSerializationUtil offers functionality to serialize and de-serialize expression nodes to the
 * corresponding protobuffer object.
 */
class ExpressionSerializationUtil {
  public:
    /**
    * @brief Serializes a expression node and all its children to a SerializableDataType object.
    * @param expressionNode The root expression node to serialize.
    * @param serializedExpression The corresponding protobuff object, which is used to capture the state of the object.
    * @return the modified serializedExpression
    */
    static SerializableExpression* serializeExpression(ExpressionNodePtr expressionNode, SerializableExpression* serializedExpression);

    /**
     * @brief De-serializes the SerializableExpression and all its children to a corresponding ExpressionNodePtr
     * @param serializedExpression the serialized expression.
     * @return ExpressionNodePtr
     */
    static ExpressionNodePtr deserializeExpression(SerializableExpression* serializedExpression);

  private:
    static void serializeLogicalExpressions(ExpressionNodePtr sharedPtr, SerializableExpression* pExpression);
    static void serializeArithmeticalExpressions(ExpressionNodePtr expression, SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeLogicalExpressions(SerializableExpression* serializedExpression);
    static ExpressionNodePtr deserializeArithmeticalExpressions(SerializableExpression* serializedExpression);
};
}// namespace NES

#endif//NES_INCLUDE_UTIL_EXPRESSIONSERIALIZATIONUTIL_HPP_
