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

#include <API/Expressions/GeographyExpressions.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>

namespace NES {

ExpressionNodePtr ST_WITHIN(const ExpressionItem& latitudeFieldName,
                            const ExpressionItem& longitudeFieldName,
                            const ExpressionItem& wkt) {
    // GeographyFieldsAccessExpressionNode for latitude and longitude fields
    auto latitudeExpression = latitudeFieldName.getExpressionNode();
    if (!latitudeExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Spatial Query: latitude has to be an FieldAccessExpression but it was a " + latitudeExpression->toString());
    }
    auto latitudeAccess = latitudeExpression->as<FieldAccessExpressionNode>();

    auto longitudeExpression = longitudeFieldName.getExpressionNode();
    if (!longitudeExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Spatial Query: latitude has to be an FieldAccessExpression but it was a " + longitudeExpression->toString());
    }
    auto longitudeAccess = longitudeExpression->as<FieldAccessExpressionNode>();

    auto geographyFieldsAccess = GeographyFieldsAccessExpressionNode::create(latitudeAccess, longitudeAccess);

    // ConstantValueExpressionNode for the wkt string
    auto wktExpression = wkt.getExpressionNode();
    if (!wktExpression->instanceOf<ConstantValueExpressionNode>()) {
        NES_ERROR("Spatial Query: wkt has to be an ConstantValueExpression but it was a " + wktExpression->toString());
    }
    auto wktConstantValueExpressionNode = longitudeExpression->as<ConstantValueExpressionNode>();

    return STWithinExpressionNode::create(std::move(geographyFieldsAccess->as<GeographyFieldsAccessExpressionNode>()), std::move(wktConstantValueExpressionNode));
}

}// namespace NES