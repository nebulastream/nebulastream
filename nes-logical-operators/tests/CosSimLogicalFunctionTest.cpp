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

#include <tuple>
#include <DataTypes/DataType.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/VectorFunctions/CosSimLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
namespace
{

class CosSimLogicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("CosSimLogicalFunctionTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CosSimLogicalFunctionTest test class.");
    }
};

TEST_F(CosSimLogicalFunctionTest, InfersFloat64AndNullabilityForVarsizedInputs)
{
    const Schema<Field, Unordered> emptySchema{};
    const ConstantValueLogicalFunction nonNullVec{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "@@@@"};
    const ConstantValueLogicalFunction nullableVec{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE}, "@@@@"};

    const auto nonNullResult = CosSimLogicalFunction{nonNullVec, nonNullVec}.withInferredDataType(emptySchema);
    EXPECT_EQ(nonNullResult.getDataType(), (DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}));

    const auto rightNullResult = CosSimLogicalFunction{nonNullVec, nullableVec}.withInferredDataType(emptySchema);
    EXPECT_EQ(rightNullResult.getDataType(), (DataType{DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE}));

    const auto leftNullResult = CosSimLogicalFunction{nullableVec, nonNullVec}.withInferredDataType(emptySchema);
    EXPECT_EQ(leftNullResult.getDataType(), (DataType{DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE}));

    const auto bothNullResult = CosSimLogicalFunction{nullableVec, nullableVec}.withInferredDataType(emptySchema);
    EXPECT_EQ(bothNullResult.getDataType(), (DataType{DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE}));
}

TEST_F(CosSimLogicalFunctionTest, EqualityIsCommutative)
{
    const ConstantValueLogicalFunction leftVec{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "@@@@"};
    const ConstantValueLogicalFunction rightVec{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "BBBB"};
    const ConstantValueLogicalFunction otherVec{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "CCCC"};

    EXPECT_TRUE((CosSimLogicalFunction{leftVec, rightVec} == CosSimLogicalFunction{leftVec, rightVec}));
    EXPECT_TRUE((CosSimLogicalFunction{leftVec, rightVec} == CosSimLogicalFunction{rightVec, leftVec}));
    EXPECT_FALSE((CosSimLogicalFunction{leftVec, rightVec} == CosSimLogicalFunction{leftVec, otherVec}));
}

TEST_F(CosSimLogicalFunctionTest, ThrowsForInvalidInputsAndArity)
{
    const Schema<Field, Unordered> emptySchema{};
    const ConstantValueLogicalFunction vecInput{DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}, "@@@@"};
    const ConstantValueLogicalFunction numInput{DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE}, "1.0"};

    ASSERT_EXCEPTION_ERRORCODE(
        (std::ignore = CosSimLogicalFunction{vecInput, numInput}.withInferredDataType(emptySchema)), ErrorCode::CannotInferStamp);
    ASSERT_EXCEPTION_ERRORCODE(
        (std::ignore = CosSimLogicalFunction{numInput, vecInput}.withInferredDataType(emptySchema)), ErrorCode::CannotInferStamp);

    ASSERT_EXCEPTION_ERRORCODE(
        (std::ignore = CosSimLogicalFunction::createCOSINE_SIMILARITY(LogicalFunctionRegistryArguments{.children = {}})),
        ErrorCode::CannotDeserialize);
    ASSERT_EXCEPTION_ERRORCODE(
        (std::ignore = CosSimLogicalFunction::createCOSINE_SIMILARITY(LogicalFunctionRegistryArguments{.children = {vecInput}})),
        ErrorCode::CannotDeserialize);
}

}
}
