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

#include <Mobility/Storage/FilterStorage.h>
#include <Mobility/Storage/LocationStorage.h>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>

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
        EXPECT_EQ_GEOPOINTS(expectedPoint, storage.getGeo(i));
    }
}

TEST(FilterStorage, FilterBuffer) {
    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[5];
    };

    NodeEngine::BufferManagerPtr bufferManager = std::make_shared<NodeEngine::BufferManager>(1024, 1024);
    auto buf = *bufferManager->getBufferNoBlocking();
    auto* my_array = buf.getBuffer<MyTuple>();

    for (unsigned int i = 0; i < 10; ++i) {
        my_array[i] = MyTuple{i, float(0.5F * i), double(i * 0.2), i * 2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
        << std::string(my_array[i].s, 5) << std::endl;
    }

    // Add duplicates
    my_array[10] = MyTuple{3, float(0.5F * 3), double(3 * 0.2), 3 * 2, "1234"};
    my_array[11] = MyTuple{4, float(0.5F * 4), double(4 * 0.2), 4 * 2, "1234"};
    my_array[12] = MyTuple{5, float(0.5F * 5), double(5 * 0.2), 5 * 2, "1234"};

    // Add  close duplicates (only one value is different)
    my_array[13] = MyTuple{6, float(0.5F * 6), double(6 * 0.2), 6 * 2, "1235"};
    my_array[14] = MyTuple{7, float(0.5F * 7), double(7 * 0.2), 7 * 2, "1235"};

    // Add duplicates after storage clean
    my_array[15] = MyTuple{0, float(0.5F * 0), double(0 * 0.2), 0 * 2, "1234"};

    buf.setNumberOfTuples(16);
    SchemaPtr s = Schema::create()
        ->addField("i64", DataTypeFactory::createUInt64())
        ->addField("f", DataTypeFactory::createFloat())
        ->addField("d", DataTypeFactory::createDouble())
        ->addField("i32", DataTypeFactory::createUInt32())
        ->addField("s", DataTypeFactory::createFixedChar(5));

    FilterStorage filterStorage(s, 10);
    NodeEngine::TupleBuffer filteredBuf = filterStorage.filter(buf);

    std::string reference = "+----------------------------------------------------+\n"
                            "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR[5]|\n"
                            "+----------------------------------------------------+\n"
                            "|0|0.000000|0.000000|0|1234 |\n"
                            "|1|0.500000|0.200000|2|1234 |\n"
                            "|2|1.000000|0.400000|4|1234 |\n"
                            "|3|1.500000|0.600000|6|1234 |\n"
                            "|4|2.000000|0.800000|8|1234 |\n"
                            "|5|2.500000|1.000000|10|1234 |\n"
                            "|6|3.000000|1.200000|12|1234 |\n"
                            "|7|3.500000|1.400000|14|1234 |\n"
                            "|8|4.000000|1.600000|16|1234 |\n"
                            "|9|4.500000|1.800000|18|1234 |\n"
                            "|6|3.000000|1.200000|12|1235 |\n"
                            "|7|3.500000|1.400000|14|1235 |\n"
                            "|0|0.000000|0.000000|0|1234 |\n"
                            "+----------------------------------------------------+";

    std::string result = UtilityFunctions::prettyPrintTupleBuffer(filteredBuf, s);
    std::cout << "RES=" << result << std::endl;
    NES_DEBUG("Reference size=" << reference.size() << " content=\n" << reference);
    NES_DEBUG("Result size=" << result.size() << " content=\n" << result);
    NES_DEBUG("----");
    EXPECT_EQ(10U, filterStorage.size());
    EXPECT_EQ(reference.size(), result.size());
}

}
