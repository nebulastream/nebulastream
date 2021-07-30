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

#include "Catalogs/GeoLocation.h"
#include "Catalogs/GeoNode.h"
#include "Catalogs/LocationCatalog.h"

namespace NES {

TEST(GeoLocation, ValidLocation) {

    GeoLocation validLocation(2,3);
    EXPECT_TRUE(validLocation.isValid());

    GeoLocation invalidLocation;
    EXPECT_FALSE(invalidLocation.isValid());
}

TEST(GeoNode, AddAndUpdateLocations) {
    GeoNode node(1);

    EXPECT_FALSE(node.getCurrentLocation().isValid());
    EXPECT_TRUE(node.getLocationHistory().empty());

    node.setCurrentLocation(GeoLocation(1,1));
    node.setCurrentLocation(GeoLocation(2,2));
    node.setCurrentLocation(GeoLocation(3,3));

    GeoLocation expectedLocation(3,3);
    EXPECT_EQ(expectedLocation, node.getCurrentLocation());

    const uint64_t expectedSize= 2;
    EXPECT_EQ(expectedSize, node.getLocationHistory().size());

}

TEST(LocationCatalog, AddNodeAndUpdateLocation) {
    LocationCatalog catalog;

    catalog.addNode(1);
    catalog.addNode(2);

    const uint64_t expectedSize = 2;
    EXPECT_EQ(expectedSize, catalog.size());

    catalog.updateNodeLocation(1, GeoLocation(1,1));
    catalog.updateNodeLocation(1, GeoLocation(1.5,1.5));
    catalog.updateNodeLocation(2, GeoLocation(2,2));

    GeoNode firstNode = catalog.getNode(1);
    EXPECT_EQ(GeoLocation(1.5,1.5), firstNode.getCurrentLocation());
    EXPECT_FALSE(firstNode.getLocationHistory().empty());

    GeoNode secondNode = catalog.getNode(2);
    EXPECT_EQ(GeoLocation(2,2), secondNode.getCurrentLocation());
    EXPECT_TRUE(secondNode.getLocationHistory().empty());
}


}
