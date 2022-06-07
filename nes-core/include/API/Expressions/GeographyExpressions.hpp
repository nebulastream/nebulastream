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

#ifndef NES_NES_CORE_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_
#define NES_NES_CORE_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_

namespace NES {

class ExpressionNode;
class ExpressionItem;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

/**
 * @brief Defines common spatial operations between expression nodes.
 */

/**
 * @brief This expression represents ST_WITHIN predicate, where ST stands for Spatial
 * Type. ST_WITHIN predicate in general defines the relationship between two objects
 * (i.e., whether a geometric object is within another geometric object or not). In NES,
 * we only expect the stream to report the GPS coordinates from a source (i.e., a Point).
 * Thus in NES, by using ST_WITHIN expression a user can filter points in the stream
 * which are within a geometric object or not. For now, the geometric object is limited
 * to a rectangle. ST_Within is supposed to be used with the filter operator as follows:
 *
 * stream.filter(ST_WITHIN(Attribute("latitude"), Attribute("longitude"), WKT_POLYGON))
 *
 * where latitude, and longitude represent the attributes lat/long in the schema, and
 * WKT_POLYGON is the well-known text (WKT) representation of a rectangle (i.e., Envelope
 * as defined by OGC: see https://postgis.net/docs/ST_Envelope.html for more details).
 *
 * WKT_POLYGON can be defined either by the bounding coordinates of the envelope
 * (i.e., (min_lat, min_long), (max_lat, max_long), or all four coordinates of the
 * envelope as defined in the link above.
 */
ExpressionNodePtr ST_WITHIN(const ExpressionItem& latitude,
                            const ExpressionItem& longitude,
                            const ExpressionItem& wkt);

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
 * stream.filter(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), WKT_POINT, distance))
 *
 * where latitude, and longitude represent the attributes lat/long in the stream, and
 * WKT_POINT is the well-known text representation of the query point, and the distance
 * is the distance in meters.
 */
ExpressionNodePtr ST_DWITHIN(const ExpressionItem& latitude,
                             const ExpressionItem& longitude,
                             const ExpressionItem& wkt,
                             const ExpressionItem& distance);

/**
 * @brief This node represents ST_KNN predicate, where ST stands for Spatial Type and
 * KNN stands for "K" nearest neighbor. In general, ST_KNN predicate retrieves the "k"
 * nearest neighbors of a query point from a group of geometric objects. In NES, we will
 * combine ST_KNN expression with a window which would allow us to define the batch of
 * objects from which to select the "k" nearest neighbors of the query point. For now,
 * this expression remains unimplemented and an error is thrown.
 */
ExpressionNodePtr ST_KNN(const ExpressionItem& latitude,
                         const ExpressionItem& longitude,
                         const ExpressionItem& wkt,
                         const ExpressionItem& k);

}// namespace NES

#endif//NES_NES_CORE_INCLUDE_API_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_HPP_
