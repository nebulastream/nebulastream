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

#include <Mobility/Storage/LocationStorage.h>

namespace NES {

void EXPECT_EQ_GEOPOINTS(GeoPoint p1, GeoPointPtr p2){
    EXPECT_EQ(p1.getLatitude(), p2->getLatitude());
    EXPECT_EQ(p1.getLongitude(), p2->getLongitude());
}

TEST(LocationStorage, ValidLocation) {
    const uint32_t storageSize  = 5;
    LocationStorage storage(storageSize);

    for (uint32_t i = 0; i < storageSize + 1; i++) {
        storage.add(std::make_shared<GeoPoint>(i, i));
    }

    EXPECT_EQ(storage.size(), storageSize);

    for (uint32_t i = 0; i < storageSize; i++) {
        GeoPoint expectedPoint(i + 1, i + 1);
        EXPECT_EQ_GEOPOINTS(expectedPoint, storage.get(i));
    }
}

}