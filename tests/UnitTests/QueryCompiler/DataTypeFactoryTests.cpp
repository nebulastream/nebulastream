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

#include "../../util/NESTest.hpp"
#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <Sources/DefaultSource.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

using namespace std;

namespace NES {

    using namespace NES::API;
    using namespace NES::QueryCompilation::PhysicalOperators;

    class DataTypeFactoryTests : public testing::Test {
    public:
        static void SetUpTestCase() {
            NES::setupLogging("DataTypeFactory.log", NES::LOG_DEBUG);
            NES_INFO("Setup DataTypeFactory test class.");
        }

        void SetUp() {}

        void TearDown() { NES_DEBUG("Tear down AddScanAndEmitPhase Test."); }
    };

/**
 * @brief Input Query Plan:
 * Input:
 * | Physical Source |
 *
 * Expected Result:
 * | Physical Source |
 *
 */
    TEST_F(DataTypeFactoryTests, stampModificationTest) {
        // test beavior of integer stamp
        {
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), (int64_t) -100, (int64_t) 100);
            ASSERT_TRUE(stamp->isInteger());
            auto intStamp = DataType::as<Integer>(stamp);
            ASSERT_EQ(intStamp->getBits(), 32);
            ASSERT_EQ(intStamp->lowerBound, -100);
            ASSERT_EQ(intStamp->upperBound, 100);
        }
        {
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), (double) -100.0, (double) 100.0);
            ASSERT_TRUE(stamp->isInteger());
            auto intStamp = DataType::as<Integer>(stamp);
            ASSERT_EQ(intStamp->getBits(), 32);
            ASSERT_EQ(intStamp->lowerBound, -100);
            ASSERT_EQ(intStamp->upperBound, 100);
        }
        {
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createInt32(), (double) -99.9, (double) 99.9);
            ASSERT_TRUE(stamp->isInteger());
            auto intStamp = DataType::as<Integer>(stamp);
            ASSERT_EQ(intStamp->getBits(), 32);
            ASSERT_EQ(intStamp->lowerBound, -100);
            ASSERT_EQ(intStamp->upperBound, 100);
        }
        // test behaviour of float stamp
        {
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createFloat(), (double) -100.5, (double) 100.5);
            ASSERT_TRUE(stamp->isFloat());
            auto floatStamp = DataType::as<Float>(stamp);
            ASSERT_EQ(floatStamp->getBits(), 32);
            ASSERT_EQ(floatStamp->lowerBound, -100.5);
            ASSERT_EQ(floatStamp->upperBound, 100.5);
        }
        {
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createFloat(), (int64_t) -100, (int64_t) 100);
            ASSERT_TRUE(stamp->isFloat());
            auto floatStamp = DataType::as<Float>(stamp);
            ASSERT_EQ(floatStamp->getBits(), 32);
            ASSERT_EQ(floatStamp->lowerBound, -100);
            ASSERT_EQ(floatStamp->upperBound, 100);
        }
        {
            // check if call with one int and one float as new bounds works
            auto stamp = DataTypeFactory::copyTypeAndTightenBounds(DataTypeFactory::createFloat(), (int64_t) -100, (double) 99.9);
            ASSERT_TRUE(stamp->isFloat());
            auto floatStamp = DataType::as<Float>(stamp);
            ASSERT_EQ(floatStamp->getBits(), 32);
            ASSERT_EQ(floatStamp->lowerBound, -100);
            ASSERT_EQ(floatStamp->upperBound, 99.9);
        }
    }

}// namespace NES