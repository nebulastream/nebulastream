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

#ifndef NES_CORE_INCLUDE_GRPC_SERIALIZATION_SHAPETYPESERIALIZATIONUTIL_HPP_
#define NES_CORE_INCLUDE_GRPC_SERIALIZATION_SHAPETYPESERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

class SerializableShapeType;

/**
 * @brief The ShapeTypeSerializationUtil serializes and de-serializes shape types to a
 * corresponding protobuf object.
 */
class ShapeTypeSerializationUtil {
  public:
    /**
     * @brief Serializes a shape expression to a SerializableShapeType object.
     * @param serializedShapeType the corresponding protobuf object
     * @return the modified serializedShapeType
     */
    static SerializableShapeType* serializeShapeType(const ShapeExpressionNodePtr& shape,
                                                     SerializableShapeType* serializedShapeType);

    /**
    * @brief De-serializes the SerializableShapeType to a ShapeExpressionNodePtr
    * @param serializedShapeType the serialized shape type.
    * @return ShapeExpressionNodePtr
    */
    static ShapeExpressionNodePtr deserializeShapeType(SerializableShapeType* serializedShapeType);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_SERIALIZATION_SHAPETYPESERIALIZATIONUTIL_HPP_
