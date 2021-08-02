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

#include "Mobility/Geo/GeoNode.h"
#include "Mobility/Geo/GeoPoint.h"
#include "Mobility/Geo/GeoSquare.h"
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

    catalog.addNode("1");
    catalog.addNode("2");

    const uint64_t expectedSize = 2;
    EXPECT_EQ(expectedSize, catalog.size());

    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1,1));
    catalog.updateNodeLocation("1", std::make_shared<GeoPoint>(1.5,1.5));
    catalog.updateNodeLocation("2", std::make_shared<GeoPoint>(2,2));

    GeoNodePtr firstNode = catalog.getNode("1");

    EXPECT_EQ_GEOPOINTS(GeoPoint(1.5,1.5), firstNode->getCurrentLocation());
    EXPECT_FALSE(firstNode->getLocationHistory().empty());

    GeoNodePtr secondNode = catalog.getNode("2");
    EXPECT_EQ_GEOPOINTS(GeoPoint(2,2), secondNode->getCurrentLocation());
    EXPECT_TRUE(secondNode->getLocationHistory().empty());
}

TEST(GeoSquare, DistanceToBound) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquare square(center, 1000);

    EXPECT_DOUBLE_EQ(250, square.getDistanceToBound());
}

TEST(GeoSquare, TestBounds) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquare square(center, 1000);

    GeoPointPtr north = square.getNorthBound();
    EXPECT_DOUBLE_EQ(center->getLongitude(), north->getLongitude());
    EXPECT_TRUE(north->getLatitude() > center->getLatitude());

    GeoPointPtr south = square.getSouthBound();
    EXPECT_DOUBLE_EQ(center->getLongitude(), south->getLongitude());
    EXPECT_TRUE(south->getLatitude() < center->getLatitude());

    GeoPointPtr east = square.getEastBound();
    EXPECT_DOUBLE_EQ(center->getLatitude(), east->getLatitude());
    EXPECT_TRUE(east->getLongitude() > center->getLongitude());

    GeoPointPtr west = square.getWestBound();
    EXPECT_DOUBLE_EQ(center->getLatitude(), west->getLatitude());
    EXPECT_TRUE(west->getLongitude() < center->getLongitude());
}

TEST(GeoSquare, TestContains) {
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    GeoSquare square(center, 1000);

    EXPECT_TRUE(square.contains(center));
    EXPECT_TRUE(    square.contains(std::make_shared<GeoPoint>(52.5128427, 13.3213395)));
    EXPECT_TRUE(    square.contains(std::make_shared<GeoPoint>(52.51508748, 13.32504968)));

    EXPECT_FALSE(    square.contains(std::make_shared<GeoPoint>(52.51508751, 13.32504971)));
}

TEST(LocationService, CheckIfLocationIsInRange) {
    LocationService service;
    GeoPointPtr center = std::make_shared<GeoPoint>(52.5128417, 13.3213595);
    service.addNode("1");
    service.updateNodeLocation("1", center);

    EXPECT_TRUE(service.checkIfPointInRange("1", 1000, std::make_shared<GeoPoint>(52.51508748, 13.32504968)));
    EXPECT_FALSE(service.checkIfPointInRange("1", 1000, std::make_shared<GeoPoint>(52.51508751, 13.32504971)));

}

}
