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
#include <Operators/Serialization/ShapeTypeSerializationUtil.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PolygonExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <SerializableShapeType.pb.h>
#include <Util/Logger/Logger.hpp>
#include <vector>

namespace NES {

SerializableShapeType* ShapeTypeSerializationUtil::serializeShapeType(const ShapeExpressionNodePtr& shape,
                                                                      SerializableShapeType* serializedShape) {
    ShapeType shapeType = shape->getShapeType();
    if (shapeType == ShapeType::Circle) {
        auto circleExpressionNode = shape->as<CircleExpressionNode>();
        serializedShape->set_shapetype(SerializableShapeType_ShapeType_CIRCLE);
        auto serializedCircle = SerializableShapeType_SerializableCircle();
        serializedCircle.set_latitude(circleExpressionNode->getLatitude());
        serializedCircle.set_longitude(circleExpressionNode->getLongitude());
        serializedCircle.set_radius(circleExpressionNode->getRadius());
        serializedShape->mutable_details()->PackFrom(serializedCircle);
    } else if (shapeType == ShapeType::Point) {
        serializedShape->set_shapetype(SerializableShapeType_ShapeType_POINT);
        auto pointExpressionNode = shape->as<PointExpressionNode>();
        auto serializedPoint = SerializableShapeType_SerializablePoint();
        serializedPoint.set_latitude(pointExpressionNode->getLatitude());
        serializedPoint.set_longitude(pointExpressionNode->getLongitude());
        serializedShape->mutable_details()->PackFrom(serializedPoint);
    } else if (shapeType == ShapeType::Polygon) {
        serializedShape->set_shapetype(SerializableShapeType_ShapeType_POLYGON);
        auto polygonExpressionNode = shape->as<PolygonExpressionNode>();
        auto serializedPolygon = SerializableShapeType_SerializablePolygon();
        auto coordinates = polygonExpressionNode->getCoordinates();
        for (auto& coord : coordinates) {
            serializedPolygon.add_coords(coord);
        }
        serializedShape->mutable_details()->PackFrom(serializedPolygon);
    } else if (shapeType == ShapeType::Rectangle) {
        auto rectangleExpressionNode = shape->as<RectangleExpressionNode>();
        serializedShape->set_shapetype(SerializableShapeType_ShapeType_RECTANGLE);
        auto serializedRectangle = SerializableShapeType_SerializableRectangle();
        serializedRectangle.set_latitudelow(rectangleExpressionNode->getLatitudeLow());
        serializedRectangle.set_longitudelow(rectangleExpressionNode->getLongitudeLow());
        serializedRectangle.set_latitudehigh(rectangleExpressionNode->getLatitudeHigh());
        serializedRectangle.set_longitudehigh(rectangleExpressionNode->getLongitudeHigh());
        serializedShape->mutable_details()->PackFrom(serializedRectangle);
    } else {
        NES_THROW_RUNTIME_ERROR("ShapeTypeSerializationUtil: serialization is not possible for " + shape->toString());
    }
    NES_TRACE("ShapeTypeSerializationUtil:: serialized {} to {}", shape->toString(), serializedShape->SerializeAsString());
    return serializedShape;
}

ShapeExpressionNodePtr ShapeTypeSerializationUtil::deserializeShapeType(SerializableShapeType* serializedShapeType) {
    NES_TRACE("ShapeTypeSerializationUtil:: de-serialized {}", serializedShapeType->DebugString());
    const auto& data = serializedShapeType->details();
    const auto shapeType = serializedShapeType->shapetype();
    if (shapeType == SerializableShapeType_ShapeType_CIRCLE) {
        auto serializedCircle = SerializableShapeType_SerializableCircle();
        data.UnpackTo(&serializedCircle);
        double latitude = serializedCircle.latitude();
        double longitude = serializedCircle.longitude();
        double radius = serializedCircle.radius();
        return CircleExpressionNode::create(latitude, longitude, radius);
    } else if (shapeType == SerializableShapeType_ShapeType_POINT) {
        auto serializedPoint = SerializableShapeType_SerializablePoint();
        data.UnpackTo(&serializedPoint);
        double latitude = serializedPoint.latitude();
        double longitude = serializedPoint.longitude();
        return PointExpressionNode::create(latitude, longitude);
    } else if (shapeType == SerializableShapeType_ShapeType_POLYGON) {
        auto serializedPolygon = SerializableShapeType_SerializablePolygon();
        data.UnpackTo(&serializedPolygon);
        std::vector<double> values;
        values.reserve(serializedPolygon.coords_size());
        for (const auto& coord : serializedPolygon.coords()) {
            values.push_back(coord);
        }
        return PolygonExpressionNode::create(std::move(values));
    } else if (shapeType == SerializableShapeType_ShapeType_RECTANGLE) {
        auto serializedRectangle = SerializableShapeType_SerializableRectangle();
        data.UnpackTo(&serializedRectangle);
        double latitudeLow = serializedRectangle.latitudelow();
        double longitudeLow = serializedRectangle.longitudelow();
        double latitudeHigh = serializedRectangle.latitudehigh();
        double longitudeHigh = serializedRectangle.longitudehigh();
        return RectangleExpressionNode::create(latitudeLow, longitudeLow, latitudeHigh, longitudeHigh);
    }

    NES_THROW_RUNTIME_ERROR("ShapeTypeSerializationUtil: shape type which is to be serialized not registered. "
                            "Deserialization is not possible");
    return nullptr;
}

}// namespace NES
