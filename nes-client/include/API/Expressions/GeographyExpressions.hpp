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

#ifndef NES_CLIENT_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_
#define NES_CLIENT_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_

namespace NES {

class ExpressionItem;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

/**
 * @brief Defines common spatial shapes.
 */

/**
 * @brief Defines a circle shape. A user can define a circle as follows:
 * CIRCLE(29.381726, 79.453078, 100.0), where the
 * @param latitude defines the latitude of the center of the circle.
 * @param longitude defines the longitude of the center of the circle.
 * @param radius defines the radius of the circle.
 *
 *               *  *
 *           *          *
 *         *      radius  *
 *         *       o------*
 *         *    (lat,lon) *
 *           *          *
 *               *  *
 */
ShapeExpressionNodePtr CIRCLE(double latitude, double longitude, double radius);

/**
 * @brief Defines a point shape. A user can define a point as follows:
 * POINT(29.393136,79.450638), where the
 * @param latitude defines the latitude of the point.
 * @param longitude defines the longitude of the point.
 */
ShapeExpressionNodePtr POINT(double latitude, double longitude);

/**
 * @brief Defines a rectangle shape. A user can define a rectangle as follows:
 * RECTANGLE(29.388829, 79.453806, 29.389843, 79.454933), where the
 * @param latitudeLow defines the latitude of the south-west (bottom-left) point
 * of the rectangle.
 * @param longitudeLow defines the longitude of the south-west (bottom-left) point
 * of the rectangle.
 * @param latitudeHigh defines the latitude of the north-east (top-right) point
 * of the rectangle.
 * @param longitudeHigh defines the longitude of the north-east (top-right) point
 * of the rectangle.
 *
 *                           o-----------------o north-east (latitudeHigh,
 *                           |                 |             longitudeHigh)
 *                           |                 |
 * south-west (latitudeLow,  o-----------------o
 *             longitudeLow)
*/
ShapeExpressionNodePtr RECTANGLE(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh);

/**
 * @brief Defines a polygon shape. The user can define a polygon as follows:
 * POLYGON({29.396018, 79.435183, 29.375910, 79.449621, 29.363619, 79.46852, 29.372501,
 * 79.475521, 29.401183, 79.451892}) where,
 * @param coords are the coordinates of the polygon.
 */
ShapeExpressionNodePtr POLYGON(std::initializer_list<double> coords);

/**
 * @brief Defines common spatial predicates.
 */

/**
 * @brief This expression represents ST_WITHIN predicate, where ST stands for Spatial
 * Type. ST_WITHIN predicate in general defines the relationship between two objects
 * (i.e., whether a geometric object is within another geometric object or not). In NES,
 * we only expect the stream to report the GPS coordinates from a source (i.e., a Point).
 * Thus in NES, by using ST_WITHIN expression a user can filter points in the stream
 * which are within a geometric shape or not. The shape can be a Circle, a Polygon, or a
 * Rectangle. If the shape is Circle we internally call ST_DWithin. ST_Within is supposed
 * to be used with the filter operator as follows:
 *
 * stream.filter(ST_WITHIN(Attribute("latitude"), Attribute("longitude"), SHAPE))
 *
 * where latitude, and longitude represent the attributes lat/long in the schema, and
 * SHAPE is one of Circle, Rectangle, or Polygon.
 * @see NES::CIRCLE
 * @see NES::POLYGON
 * @see NES::RECTANGLE
 *
 * @param latitude is the latitude attribute in the stream.
 * @param longitude is the longitude attribute in the stream.
 * @param shapeExpression is one of Circle, Polygon or Rectangle shape.
 *
 * @throws InvalidArgumentException when one of the arguments is invalid.
 */
ExpressionNodePtr
ST_WITHIN(const ExpressionItem& latitude, const ExpressionItem& longitude, const ShapeExpressionNodePtr& shapeExpression);

/**
 * @brief This expression represents ST_DWithin predicate, where ST stands for Spatial
 * type, and DWithin stands for distance within. In general, the ST_DWITHIN predicate
 * defines whether a geometric object is within distance "d" of another geometric object.
 * In NES, we only expect the stream to report the GPS coordinates from a source (i.e.,
 * a Point). Thus in NES, by using ST_DWITHIN predicate a user can filter points in the
 * stream which are within distance "d" of the specified geometric object. For now, the
 * geometric object is limited to a point, and the distance is assumed to be in meters.
 * ST_DWithin is supposed to be used with the filter operator as follows:
 *
 * stream.filter(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), SHAPE)
 *
 * where latitude, and longitude represent the attributes lat/long in the stream, and
 * SHAPE is a circle which constitutes of a lat/lon coordinate and a radius (radius
 * defines the distance for the ST_DWITHIN predicate).
 * @see NES::CIRCLE
 *
 * @param latitude is the latitude attribute in the stream.
 * @param longitude is the longitude attribute in the stream.
 * @param shapeExpression is a Circle shape, radius of which is the distance "D".
 *
 * @throws InvalidArgumentException when one of the arguments is invalid.
 */
ExpressionNodePtr
ST_DWITHIN(const ExpressionItem& latitude, const ExpressionItem& longitude, const ShapeExpressionNodePtr& shapeExpression);

/**
 * @brief This node represents ST_KNN predicate, where ST stands for Spatial Type and
 * KNN stands for "K" nearest neighbor. In general, ST_KNN predicate retrieves the "k"
 * nearest neighbors of a query point from a group of geometric objects. In NES, we will
 * combine ST_KNN expression with a window which would allow us to define the batch of
 * objects from which to select the "k" nearest neighbors of the query point. For now,
 * this expression remains unimplemented and an error is thrown. To define the query
 * point:
 * @see NES::POINT
 *
 * @param latitude is the latitude attribute in the stream.
 * @param longitude is the longitude attribute in the stream.
 * @param shapeExpression is the query point (a point shape).
 * @param k is the variable K in KNN query.
 *
 * @throws InvalidArgumentException when one of the arguments is invalid.
 */
ExpressionNodePtr ST_KNN(const ExpressionItem& latitude,
                         const ExpressionItem& longitude,
                         const ShapeExpressionNodePtr& shapeExpression,
                         const ExpressionItem& k);

}// namespace NES

#endif// NES_CLIENT_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_
