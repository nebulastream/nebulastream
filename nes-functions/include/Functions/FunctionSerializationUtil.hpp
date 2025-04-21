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

#pragma once

#include "Expression.hpp"

namespace NES
{


class SerializableFunction;
class SerializableFunction_FunctionConstantValue;
class SerializableFunction_FunctionFieldAccess;
class SerializableFunction_FunctionFieldRename;
class SerializableFunction_FunctionFieldAssignment;
class SerializableFunction_FunctionWhen;
class SerializableFunction_FunctionCase;
class SerializableFunction_FunctionFunction;

/**
* @brief The FunctionSerializationUtil offers functionality to serialize and de-serialize function nodes to the
* corresponding protobuffer object.
*/
class FunctionSerializationUtil
{
public:
    /**
    * @brief Serializes a function node and all its children to a SerializableDataType object.
    * @param nodeFunction The root function node to serialize.
    * @param serializedFunction The corresponding protobuff object, which is used to capture the state of the object.
    * @return the modified serializedFunction
    */
    static void serializeExpression(const ExpressionValue& nodeFunction, SerializableFunction* serializedFunction);

    /**
    * @brief De-serializes the SerializableFunction and all its children to a corresponding std::shared_ptr<NodeFunction>
    * @param serializedFunction the serialized function.
    * @return std::shared_ptr<NodeFunction>
    */
    static ExpressionValue deserializeExpression(const SerializableFunction& serializedFunction);
};
}
