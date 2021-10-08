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

#include "gtest/gtest.h"

#include <Mobility/Geo/Area/GeoAreaFactory.h>
#include <Mobility/Geo/Area/GeoCircle.h>
#include <Mobility/Geo/Area/GeoSquare.h>
#include <Mobility/Geo/GeoPoint.h>
#include <Mobility/Geo/Node/GeoNode.h>
#include <Mobility/Geo/Projection/GeoCalculator.h>
#include <Mobility/LocationCatalog.h>
#include <Mobility/LocationService.h>

namespace NES {

const static uint32_t defaultStorageSize = 5;

void EXPECT_EQ_GEOPOINTS(GeoPoint p1, GeoPointPtr p2){
    EXPECT_EQ(p1.getLatitude(), p2->getLatitude());
    EXPECT_EQ(p1.getLongitude(), p2->getLongitude());
}

TEST(GeoLocation, ValidLocation) {
    GeoPoint validLocation(2,3);
    EXPECT_TRUE(validLocation.isValid());

    GeoPoint invalidLocation;
    EXPECT_FALSE(invalidLocation.isValid());
}

TEST(GeoNode, AddAndUpdateLocations) {
    GeoNode node("1", defaultStorageSize);
    EXPECT_FALSE(node.getCurrentLocation()->isValid());
    EXPECT_TRUE(node.getLocationHistory()->empty());

    node.setCurrentLocation(std::make_shared<GeoPoint>(1,1));
    node.setCurrentLocation(std::make_shared<GeoPoint>(2,2));
    node.setCurrentLocation(std::make_shared<GeoPoint>(3,3));

    GeoPoint expectedLocation(3,3);
    EXPECT_EQ_GEOPOINTS(expectedLocation, node.getCurrentLocation());

    const uint64_t expectedSize= 3;
    EXPECT_EQ(expectedSize, node.getLocationHistory()->size());
}

TEST(LocationCatalog, AddNodeAndUpdateLocation) {
    LocationCatalog catalog(defaultStorageSize);

    catalog.addSource("1");
    catalog.addSource("2");
    catalog.addSource("3", 500);
    catalog.addSink("4", 1000);

    EXPECT_EQ(4U, catalog.size());

    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1,1));
    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1.5,1.5));
    catalog.updateNodeLocation("2", std::make_shared<GeoPoint>(2,2));
    catalog.updateNodeLocation("3", std::make_shared<GeoPoint>(3,3));
    catalog.updateNodeLocation("4", std::make_shared<GeoPoint>(4,4));

    GeoSourcePtr firstNode = catalog.getSource("1");
    EXPECT_EQ_GEOPOINTS(GeoPoint(1.5,1.5), firstNode->getCurrentLocation());
    EXPECT_FALSE(firstNode->getLocationHistory()->empty());
    EXPECT_FALSE(firstNode->isEnabled());
    EXPECT_FALSE(firstNode->hasRange());

    GeoSourcePtr secondNode = catalog.getSource("2");
    EXPECT_EQ_GEOPOINTS(GeoPoint(2,2), secondNode->getCurrentLocation());
    EXPECT_FALSE(secondNode->getLocationHistory()->empty());
    EXPECT_FALSE(secondNode->isEnabled());
    EXPECT_FALSE(secondNode->hasRange());

    GeoSourcePtr sourceWithRange = catalog.getSource("3");
    EXPECT_EQ_GEOPOINTS(GeoPoint(3,3), sourceWithRange->getCurrentLocation());
    EXPECT_FALSE(sourceWithRange->getLocationHistory()->empty());
    EXPECT_FALSE(sourceWithRange->isEnabled());
    EXPECT_TRUE(sourceWithRange->hasRange());

    GeoSinkPtr sink = catalog.getSink("4");
    EXPECT_EQ_GEOPOINTS(GeoPoint(4,4), sink->getCurrentLocation());
    EXPECT_FALSE(sink->getLocationHistory()->empty());
}

TEST(GeoSquare, DistanceToBound) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquarePtr geoSquare = GeoAreaFactory::createSquare(center, 10000);

    EXPECT_DOUBLE_EQ(50, geoSquare->getDistanceToBound());
}

TEST(GeoSquare, TestBounds) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquarePtr geoSquare = GeoAreaFactory::createSquare(center, 10000);

    GeoPointPtr southWest = geoSquare->getGeoEdges()->getSouthWest();
    EXPECT_TRUE(southWest->getLongitude() < center->getLongitude());
    EXPECT_TRUE(southWest->getLatitude() < center->getLatitude());

    GeoPointPtr southEast = geoSquare->getGeoEdges()->getSouthEast();
    EXPECT_TRUE(southEast->getLongitude() > center->getLongitude());
    EXPECT_TRUE(southEast->getLatitude() < center->getLatitude());

    GeoPointPtr northWest = geoSquare->getGeoEdges()->getNorthWest();
    EXPECT_TRUE(northWest->getLongitude() < center->getLongitude());
    EXPECT_TRUE(northWest->getLatitude() > center->getLatitude());

    GeoPointPtr northEast = geoSquare->getGeoEdges()->getNorthEast();
    EXPECT_TRUE(northEast->getLongitude() > center->getLongitude());
    EXPECT_TRUE(northEast->getLatitude() > center->getLatitude());
}

TEST(GeoSquare, TestContainsPoint) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquarePtr geoSquare = GeoAreaFactory::createSquare(center, 10000);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    EXPECT_TRUE(geoSquare->contains(center));

    const double insideOffset = 50;
    const double outsideOffset = 51;

    CartesianPointPtr cartesianInsidePoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + insideOffset, cartesianCenter->getY() + insideOffset);
    GeoPointPtr insidePoint = GeoCalculator::cartesianToGeographic(cartesianInsidePoint);
    EXPECT_TRUE(geoSquare->contains(insidePoint));

    cartesianInsidePoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() - insideOffset, cartesianCenter->getY() - insideOffset);
    insidePoint = GeoCalculator::cartesianToGeographic(cartesianInsidePoint);
    EXPECT_TRUE(geoSquare->contains(insidePoint));

    CartesianPointPtr outsideEast = std::make_shared<CartesianPoint>(cartesianCenter->getX() + outsideOffset, cartesianCenter->getY());
    EXPECT_FALSE(geoSquare->contains(GeoCalculator::cartesianToGeographic(outsideEast)));

    CartesianPointPtr outsideWest = std::make_shared<CartesianPoint>(cartesianCenter->getX() - outsideOffset, cartesianCenter->getY());
    EXPECT_FALSE(geoSquare->contains(GeoCalculator::cartesianToGeographic(outsideWest)));

    CartesianPointPtr outsideNorth = std::make_shared<CartesianPoint>(cartesianCenter->getX(), cartesianCenter->getY() + outsideOffset);
    EXPECT_FALSE(geoSquare->contains(GeoCalculator::cartesianToGeographic(outsideNorth)));

    CartesianPointPtr outsideSouth = std::make_shared<CartesianPoint>(cartesianCenter->getX(), cartesianCenter->getY() - outsideOffset);
    EXPECT_FALSE(geoSquare->contains(GeoCalculator::cartesianToGeographic(outsideSouth)));
}

TEST(GeoSquare, TestContainsArea) {
    const double area = 10000;
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    GeoSquarePtr geoSquare = GeoAreaFactory::createSquare(center, area);
    const double circleArea = std::pow(50, 2) * M_PI;

    GeoCirclePtr circle = GeoAreaFactory::createCircle(center, circleArea);
    EXPECT_TRUE(geoSquare->contains(circle));

    const double insideOffset = 40;
    CartesianPointPtr center2 = std::make_shared<CartesianPoint>(geoSquare->getCartesianCenter()->getX() + insideOffset, geoSquare->getCartesianCenter()->getY() + insideOffset);
    circle->setCenter(GeoCalculator::cartesianToGeographic(center2));
    EXPECT_TRUE(geoSquare->contains(circle));

    const double outsideOffset = 101;
    CartesianPointPtr center3 = std::make_shared<CartesianPoint>(geoSquare->getCartesianCenter()->getX() + outsideOffset, cartesianCenter->getY() + outsideOffset);
    circle->setCenter(GeoCalculator::cartesianToGeographic(center3));
    EXPECT_FALSE(geoSquare->contains(circle));
}

TEST(GeoCircle, TestContains) {
    GeoPointPtr center = std::make_shared<GeoPoint>(10, 10);
    GeoCirclePtr circle = GeoAreaFactory::createCircle(center, 100);

    EXPECT_TRUE(circle->contains(center));
    EXPECT_TRUE(circle->contains(std::make_shared<GeoPoint>(6, 10)));
    EXPECT_TRUE(circle->contains(std::make_shared<GeoPoint>(15, 10)));

    EXPECT_FALSE(circle->contains(std::make_shared<GeoPoint>(16, 16)));
}


TEST(LocationCatalog, UpdateSources) {
    LocationCatalog catalog(defaultStorageSize);

    // Add sink
    catalog.addSink("test_sink", 10'000);
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    catalog.updateNodeLocation("test_sink", center);

    // Add source inside range
    catalog.addSource("test_source");
    CartesianPointPtr cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 40, cartesianCenter->getY());
    catalog.updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));
    catalog.updateSources();
    EXPECT_TRUE(catalog.getSource("test_source")->isEnabled());

    // Move source outside range
    cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 60, cartesianCenter->getY());
    catalog.updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));
    catalog.updateSources();
    EXPECT_FALSE(catalog.getSource("test_source")->isEnabled());

    // Move sink closer to the source
    CartesianPointPtr newCenter = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 40, cartesianCenter->getY());
    catalog.updateNodeLocation("test_sink", GeoCalculator::cartesianToGeographic(newCenter));
    catalog.updateSources();
    EXPECT_TRUE(catalog.getSource("test_source")->isEnabled());
}

TEST(LocationCatalog, UpdateSourcesWithRange) {
    LocationCatalog catalog(defaultStorageSize);

    // Add sink
    catalog.addSink("test_sink", 10'000);
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    catalog.updateNodeLocation("test_sink", center);

    // Add source inside range
    catalog.addSource("test_source", 10'000);
    CartesianPointPtr cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 60, cartesianCenter->getY());
    catalog.updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));
    catalog.updateSources();
    EXPECT_TRUE(catalog.getSource("test_source")->isEnabled());

    // Move source outside range
    cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 110, cartesianCenter->getY());
    catalog.updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));
    catalog.updateSources();
    EXPECT_FALSE(catalog.getSource("test_source")->isEnabled());

    // Move sink closer to the source
    CartesianPointPtr newCenter = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 40, cartesianCenter->getY());
    catalog.updateNodeLocation("test_sink", GeoCalculator::cartesianToGeographic(newCenter));
    catalog.updateSources();
    EXPECT_TRUE(catalog.getSource("test_source")->isEnabled());
}

TEST(LocationCatalog, PredictedSources) {
    LocationCatalog catalog(defaultStorageSize, true);

    // Add sink
    catalog.addSink("test_sink", 10'000);
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    CartesianPointPtr cartesianCenter = GeoCalculator::geographicToCartesian(center);
    catalog.updateNodeLocation("test_sink", center);

    // Add source outside range
    catalog.addSource("test_source", 10'000);
    CartesianPointPtr cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 110, cartesianCenter->getY());
    catalog.updateNodeLocation("test_source", GeoCalculator::cartesianToGeographic(cartesianPoint));
    catalog.updateSources();
    EXPECT_FALSE(catalog.getSource("test_source")->isEnabled());

    cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 10, cartesianCenter->getY());
    catalog.updateNodeLocation("test_sink", GeoCalculator::cartesianToGeographic(cartesianPoint));

    cartesianPoint = std::make_shared<CartesianPoint>(cartesianCenter->getX() + 20, cartesianCenter->getY());
    catalog.updateNodeLocation("test_sink", GeoCalculator::cartesianToGeographic(cartesianPoint));

    GeoSinkPtr sink = catalog.getSink("test_sink");
    EXPECT_FALSE(sink->getPredictedSources().empty());
    EXPECT_EQ(sink->getPredictedSources().at(0)->getSource(), catalog.getSource("test_source"));
}

}
