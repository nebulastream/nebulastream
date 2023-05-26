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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_SHAPEEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_SHAPEEXPRESSIONNODE_HPP_

#include <Nodes/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <memory>

namespace NES {

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

enum class ShapeType : uint8_t { Point, Circle, Rectangle, Polygon };

/**
 * @brief this indicates an expression which is a parameter or a GeographicExpression
 * Each expression defines a shape type. A shape type can be either point, circle,
 * rectangle, or a polygon.
 */
class ShapeExpressionNode : public Node, public GeographyExpressionNode {

  public:
    explicit ShapeExpressionNode(ShapeType shapeType);

    ~ShapeExpressionNode() = default;

    /**
     * @brief returns the shape type of this expression.
     * @return ShapeType
     */
    ShapeType getShapeType() const;

    virtual std::string toString() const = 0;

    /**
     * @brief Create a deep copy of this shape expression node.
     * @return ShapeExpressionNodePtr
     */
    virtual ShapeExpressionNodePtr copy() = 0;

  protected:
    explicit ShapeExpressionNode(ShapeExpressionNode* other);

    /**
     * @brief declares the shape type of this expression.
     */
    ShapeType type;
};
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_SHAPEEXPRESSIONNODE_HPP_
