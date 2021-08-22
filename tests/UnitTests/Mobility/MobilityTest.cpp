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
#include "Mobility/Geo/Area/GeoCircle.h"
#include "Mobility/Geo/Area/GeoSquare.h"
#include <Mobility/Geo/GeoPoint.h>
#include <Mobility/Geo/Node/GeoNode.h>
#include "Mobility/Geo/Projection/GeoCalculator.h"
#include "Mobility/LocationCatalog.h"
#include "Mobility/LocationService.h"

namespace NES {

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
    GeoNode node("1");
    EXPECT_FALSE(node.getCurrentLocation()->isValid());
    EXPECT_TRUE(node.getLocationHistory().empty());

    node.setCurrentLocation(std::make_shared<GeoPoint>(1,1));
    node.setCurrentLocation(std::make_shared<GeoPoint>(2,2));
    node.setCurrentLocation(std::make_shared<GeoPoint>(3,3));

    GeoPoint expectedLocation(3,3);
    EXPECT_EQ_GEOPOINTS(expectedLocation, node.getCurrentLocation());

    const uint64_t expectedSize= 2;
    EXPECT_EQ(expectedSize, node.getLocationHistory().size());
}

TEST(LocationCatalog, AddNodeAndUpdateLocation) {
    LocationCatalog catalog{};

    catalog.addSource("1");
    catalog.addSource("2");
    catalog.addSink("3", 1000);

    EXPECT_EQ(3U, catalog.size());

    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1,1));
    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1.5,1.5));
    catalog.updateNodeLocation("2", std::make_shared<GeoPoint>(2,2));
    catalog.updateNodeLocation("3", std::make_shared<GeoPoint>(3,3));

    GeoSourcePtr firstNode = catalog.getSource("1");

    EXPECT_EQ_GEOPOINTS(GeoPoint(1.5,1.5), firstNode->getCurrentLocation());
    EXPECT_FALSE(firstNode->getLocationHistory().empty());

    GeoSourcePtr secondNode = catalog.getSource("2");
    EXPECT_EQ_GEOPOINTS(GeoPoint(2,2), secondNode->getCurrentLocation());
    EXPECT_TRUE(secondNode->getLocationHistory().empty());

    GeoSinkPtr sink = catalog.getSink("3");
    EXPECT_EQ_GEOPOINTS(GeoPoint(3,3), sink->getCurrentLocation());
    EXPECT_TRUE(sink->getLocationHistory().empty());
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


    EXPECT_TRUE(geoSquare->contains(center));
    EXPECT_TRUE(geoSquare->contains(std::make_shared<GeoPoint>(52.5128427, 13.3213395)));
    EXPECT_FALSE(geoSquare->contains(std::make_shared<GeoPoint>(52.4876074, 13.3206216)));
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

    const double outsideOffset = 100;
    CartesianPointPtr center3 = std::make_shared<CartesianPoint>(geoSquare->getCartesianCenter()->getX() + outsideOffset, cartesianCenter->getY());
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
    LocationCatalog catalog{};

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

}
