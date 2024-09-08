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

#include <memory>

namespace NES
{

class FunctionNode;
using FunctionNodePtr = std::shared_ptr<FunctionNode>;

class SerializableFunction;
class SerializableFunction_ConstantValueFunction;
class SerializableFunction_FieldAccessFunction;
class SerializableFunction_FieldRenameFunction;
class SerializableFunction_FieldAssignmentFunction;
class SerializableFunction_WhenFunction;
class SerializableFunction_CaseFunction;
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
    * @param functionNode The root function node to serialize.
    * @param serializedFunction The corresponding protobuff object, which is used to capture the state of the object.
    * @return the modified serializedFunction
    */
    static SerializableFunction*
    serializeFunction(const FunctionNodePtr& functionNode, SerializableFunction* serializedFunction);

    /**
    * @brief De-serializes the SerializableFunction and all its children to a corresponding FunctionNodePtr
    * @param serializedFunction the serialized function.
    * @return FunctionNodePtr
    */
    static FunctionNodePtr deserializeFunction(const SerializableFunction& serializedFunction);

private:
    static void serializeLogicalFunctions(const FunctionNodePtr& function, SerializableFunction* serializedFunction);
    static void serializeArithmeticalFunctions(const FunctionNodePtr& function, SerializableFunction* serializedFunction);
    static FunctionNodePtr deserializeLogicalFunctions(const SerializableFunction& serializedFunction);
    static FunctionNodePtr deserializeArithmeticalFunctions(const SerializableFunction& serializedFunction);
};
} /// namespace NES
