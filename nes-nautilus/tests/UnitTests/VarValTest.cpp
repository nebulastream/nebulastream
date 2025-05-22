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

#include <memory>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <BaseUnitTest.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES
{
class VarValTest : public Testing::BaseUnitTest
{
public:
    /// Defining some constexpr values for more readable tests
    static constexpr auto someRandomNumber = 23.0;
    static constexpr auto minI8Minus1 = static_cast<int16_t>(std::numeric_limits<int8_t>::min()) - 1;
    static constexpr auto minI16Minus1 = static_cast<int32_t>(std::numeric_limits<int16_t>::min()) - 1;
    static constexpr auto minI32Minus1 = static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1;
    static constexpr auto maxUI8Plus1 = static_cast<uint16_t>(std::numeric_limits<uint8_t>::max()) + 1;
    static constexpr auto maxUI16Plus1 = static_cast<uint32_t>(std::numeric_limits<uint16_t>::max()) + 1;
    static constexpr auto maxUI32Plus1 = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;

    static void SetUpTestCase()
    {
        Logger::setupLogging("VarValTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup VarValTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down VarValTest class."); }
};

TEST_F(VarValTest, SimpleConstruction)
{
    auto testVarValConstruction = []<typename T>(const T value)
    {
        using namespace NES::Nautilus;
        const VarVal varVal = nautilus::val<T>(value);
        EXPECT_EQ(varVal.cast<nautilus::val<T>>(), value);
        return 0;
    };
    testVarValConstruction.operator()<int8_t>(-someRandomNumber);
    testVarValConstruction.operator()<int16_t>(minI8Minus1);
    testVarValConstruction.operator()<int32_t>(minI16Minus1);
    testVarValConstruction.operator()<int64_t>(minI32Minus1);
    testVarValConstruction.operator()<uint8_t>(someRandomNumber);
    testVarValConstruction.operator()<uint16_t>(maxUI8Plus1);
    testVarValConstruction.operator()<uint32_t>(maxUI16Plus1);
    testVarValConstruction.operator()<uint64_t>(maxUI32Plus1);
    testVarValConstruction.operator()<float>(someRandomNumber);
    testVarValConstruction.operator()<double>(someRandomNumber);
    testVarValConstruction.operator()<bool>(true);
    testVarValConstruction.operator()<bool>(false);
}

TEST_F(VarValTest, SimpleMove)
{
    auto testVarValMove = []<typename T>(const T value)
    {
        using namespace NES::Nautilus;
        const VarVal varVal = nautilus::val<T>(value);
        const VarVal varValCopy = varVal;
        const VarVal varValMove = std::move(varVal);
        EXPECT_EQ(varValMove.cast<nautilus::val<T>>(), value);
        EXPECT_EQ(varValCopy.cast<nautilus::val<T>>(), value);
        return 0;
    };

    testVarValMove.operator()<int8_t>(-someRandomNumber);
    testVarValMove.operator()<int16_t>(minI8Minus1);
    testVarValMove.operator()<int32_t>(minI16Minus1);
    testVarValMove.operator()<int64_t>(minI32Minus1);
    testVarValMove.operator()<uint8_t>(someRandomNumber);
    testVarValMove.operator()<uint16_t>(maxUI8Plus1);
    testVarValMove.operator()<uint32_t>(maxUI16Plus1);
    testVarValMove.operator()<uint64_t>(maxUI32Plus1);
    testVarValMove.operator()<float>(someRandomNumber);
    testVarValMove.operator()<double>(someRandomNumber);
    testVarValMove.operator()<bool>(true);
    testVarValMove.operator()<bool>(false);
}

#define TEST_BINARY_OPERATION_INTEGER(op) \
    { \
        auto testVarValArithOperation = []<typename T>(const T value1, const T value2) -> int \
        { \
            using namespace NES::Nautilus; \
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.cast<ResultType>(), static_cast<T>(value1 op value2)); \
                return 0; \
            } \
            else \
            { \
                return 0; \
            } \
        }; \
        constexpr auto someRandomNumber = 23.0; \
        testVarValArithOperation.operator()<int8_t>(-someRandomNumber, someRandomNumber); \
        testVarValArithOperation.operator()<int16_t>(minI8Minus1, someRandomNumber); \
        testVarValArithOperation.operator()<int32_t>(minI16Minus1, someRandomNumber); \
        testVarValArithOperation.operator()<int64_t>(minI32Minus1, someRandomNumber); \
        testVarValArithOperation.operator()<uint8_t>(someRandomNumber, someRandomNumber); \
        testVarValArithOperation.operator()<uint16_t>(maxUI8Plus1, someRandomNumber); \
        testVarValArithOperation.operator()<uint32_t>(maxUI16Plus1, someRandomNumber); \
        testVarValArithOperation.operator()<uint64_t>(maxUI32Plus1, someRandomNumber); \
    }

#define TEST_BINARY_OPERATION_FLOATING(op) \
    { \
        auto testVarValLogicalOperation = []<typename T>(const T value1, const T value2) -> int \
        { \
            using namespace NES::Nautilus; \
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.cast<ResultType>(), static_cast<T>(value1 op value2)); \
                return 0; \
            } \
            else \
            { \
                return 0; \
            } \
        }; \
        constexpr auto someRandomNumber = 23.0; \
        testVarValLogicalOperation.operator()<float>(someRandomNumber, -someRandomNumber); \
        testVarValLogicalOperation.operator()<double>(someRandomNumber, -someRandomNumber); \
    }

#define TEST_BINARY_OPERATION_BOOLEAN(op) \
    { \
        auto testVarValLogicalOperation = []<typename T>(const T value1, const T value2) -> int \
        { \
            using namespace NES::Nautilus; \
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.cast<ResultType>(), static_cast<T>(value1 op value2)); \
                return 0; \
            } \
            else \
            { \
                return 0; \
            } \
        }; \
        testVarValLogicalOperation.operator()<bool>(false, false); \
        testVarValLogicalOperation.operator()<bool>(false, true); \
        testVarValLogicalOperation.operator()<bool>(true, false); \
        testVarValLogicalOperation.operator()<bool>(true, true); \
    }

TEST_F(VarValTest, binaryOperatorOverloads)
{
    /// Testing operations defined on integer
    TEST_BINARY_OPERATION_INTEGER(*);
    TEST_BINARY_OPERATION_INTEGER(+);
    TEST_BINARY_OPERATION_INTEGER(-);
    TEST_BINARY_OPERATION_INTEGER(/);
    TEST_BINARY_OPERATION_INTEGER(&);
    TEST_BINARY_OPERATION_INTEGER(|);
    TEST_BINARY_OPERATION_INTEGER(^);
    TEST_BINARY_OPERATION_INTEGER(<);
    TEST_BINARY_OPERATION_INTEGER(>);
    TEST_BINARY_OPERATION_INTEGER(<=);
    TEST_BINARY_OPERATION_INTEGER(>=);
    TEST_BINARY_OPERATION_INTEGER(==);
    TEST_BINARY_OPERATION_INTEGER(!=);

    /// Testing operations defined on floating types
    TEST_BINARY_OPERATION_FLOATING(*);
    TEST_BINARY_OPERATION_FLOATING(+);
    TEST_BINARY_OPERATION_FLOATING(-);
    TEST_BINARY_OPERATION_FLOATING(/);
    TEST_BINARY_OPERATION_FLOATING(<);
    TEST_BINARY_OPERATION_FLOATING(>);
    TEST_BINARY_OPERATION_FLOATING(<=);
    TEST_BINARY_OPERATION_FLOATING(>=);
    TEST_BINARY_OPERATION_FLOATING(==);
    TEST_BINARY_OPERATION_FLOATING(!=);

    /// Testing operations defined on boolean types
    TEST_BINARY_OPERATION_BOOLEAN(==);
    TEST_BINARY_OPERATION_BOOLEAN(!=);
    TEST_BINARY_OPERATION_BOOLEAN(&&);
    TEST_BINARY_OPERATION_BOOLEAN(||);
}

TEST_F(VarValTest, unaryOperatorOverloads)
{
    auto testVarValOperation = []<typename T>(const T value)
    {
        using namespace NES::Nautilus;
        const VarVal varVal = nautilus::val<T>(value);
        const VarVal result = !varVal;
        EXPECT_EQ(result.cast<nautilus::val<bool>>(), !value);
        return 0;
    };

    testVarValOperation.operator()<int8_t>(-someRandomNumber);
    testVarValOperation.operator()<int16_t>(minI8Minus1);
    testVarValOperation.operator()<int32_t>(minI16Minus1);
    testVarValOperation.operator()<int64_t>(minI32Minus1);
    testVarValOperation.operator()<uint8_t>(someRandomNumber);
    testVarValOperation.operator()<uint16_t>(maxUI8Plus1);
    testVarValOperation.operator()<uint32_t>(maxUI16Plus1);
    testVarValOperation.operator()<uint64_t>(maxUI32Plus1);
    testVarValOperation.operator()<float>(someRandomNumber);
    testVarValOperation.operator()<double>(someRandomNumber);
    testVarValOperation.operator()<bool>(true);
    testVarValOperation.operator()<bool>(false);
}

TEST_F(VarValTest, writeToMemoryTest)
{
    auto testVarValWriteToMemory = []<typename T>(const T value)
    {
        using namespace NES::Nautilus;
        const VarVal varVal = nautilus::val<T>(value);
        std::vector<int8_t> memory(sizeof(T));
        const auto memoryRef = nautilus::val<int8_t*>(memory.data());
        varVal.writeToMemory(memoryRef);
        T valueFromMemory;
        std::memcpy(&valueFromMemory, memory.data(), sizeof(T));
        EXPECT_EQ(valueFromMemory, value);
        return 0;
    };

    testVarValWriteToMemory.operator()<int8_t>(-someRandomNumber);
    testVarValWriteToMemory.operator()<int16_t>(minI8Minus1);
    testVarValWriteToMemory.operator()<int32_t>(minI16Minus1);
    testVarValWriteToMemory.operator()<int64_t>(minI32Minus1);
    testVarValWriteToMemory.operator()<uint8_t>(someRandomNumber);
    testVarValWriteToMemory.operator()<uint16_t>(maxUI8Plus1);
    testVarValWriteToMemory.operator()<uint32_t>(maxUI16Plus1);
    testVarValWriteToMemory.operator()<uint64_t>(maxUI32Plus1);
    testVarValWriteToMemory.operator()<float>(someRandomNumber);
    testVarValWriteToMemory.operator()<double>(someRandomNumber);
    testVarValWriteToMemory.operator()<bool>(true);
    testVarValWriteToMemory.operator()<bool>(false);
}

TEST_F(VarValTest, readFromMemoryTest)
{
    auto testVarValReadFromMemory = []<typename T>(const T value, const std::shared_ptr<PhysicalType>& type)
    {
        using namespace NES::Nautilus;
        std::vector<int8_t> memory(sizeof(T));
        std::memcpy(memory.data(), &value, sizeof(T));
        const VarVal varVal = VarVal::readVarValFromMemory(memory.data(), type);
        EXPECT_EQ(varVal.cast<nautilus::val<T>>(), value);
        return 0;
    };

    const DefaultPhysicalTypeFactory factory;
    testVarValReadFromMemory.operator()<int8_t>(
        -someRandomNumber, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::INT8)));
    testVarValReadFromMemory.operator()<int16_t>(
        minI8Minus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::INT16)));
    testVarValReadFromMemory.operator()<int32_t>(
        minI16Minus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::INT32)));
    testVarValReadFromMemory.operator()<int64_t>(
        minI32Minus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::INT64)));
    testVarValReadFromMemory.operator()<uint8_t>(
        someRandomNumber, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT8)));
    testVarValReadFromMemory.operator()<uint16_t>(
        maxUI8Plus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT16)));
    testVarValReadFromMemory.operator()<uint32_t>(
        maxUI16Plus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT32)));
    testVarValReadFromMemory.operator()<uint64_t>(
        maxUI32Plus1, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::UINT64)));
    testVarValReadFromMemory.operator()<float>(
        someRandomNumber, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::FLOAT32)));
    testVarValReadFromMemory.operator()<double>(
        someRandomNumber, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::FLOAT64)));
    testVarValReadFromMemory.operator()<bool>(true, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)));
    testVarValReadFromMemory.operator()<bool>(false, factory.getPhysicalType(DataTypeProvider::provideDataType(LogicalType::BOOLEAN)));
}

TEST_F(VarValTest, operatorBoolTest)
{
    auto testVarValOperatorBool = []<typename T>(const T value, const bool expectedValue)
    {
        using namespace NES::Nautilus;
        const VarVal varVal = nautilus::val<T>(value);
        const auto varValBool = static_cast<bool>(varVal);
        EXPECT_EQ(varValBool, expectedValue);
        return 0;
    };

    testVarValOperatorBool.operator()<int8_t>(12, true);
    testVarValOperatorBool.operator()<int8_t>(0, false);
    testVarValOperatorBool.operator()<int16_t>(-1, true);
    testVarValOperatorBool.operator()<int16_t>(0, false);
    testVarValOperatorBool.operator()<int32_t>(-1, true);
    testVarValOperatorBool.operator()<int32_t>(0, false);
    testVarValOperatorBool.operator()<int64_t>(-1, true);
    testVarValOperatorBool.operator()<int64_t>(0, false);
    testVarValOperatorBool.operator()<uint8_t>(12, true);
    testVarValOperatorBool.operator()<uint8_t>(0, false);
    testVarValOperatorBool.operator()<uint16_t>(12, true);
    testVarValOperatorBool.operator()<uint16_t>(0, false);
    testVarValOperatorBool.operator()<uint32_t>(12, true);
    testVarValOperatorBool.operator()<uint32_t>(0, false);
    testVarValOperatorBool.operator()<uint64_t>(12, true);
    testVarValOperatorBool.operator()<uint64_t>(0, false);
    testVarValOperatorBool.operator()<float>(12.0, true);
    testVarValOperatorBool.operator()<float>(0.0, false);
    testVarValOperatorBool.operator()<double>(12.0, true);
    testVarValOperatorBool.operator()<double>(0.0, false);
    testVarValOperatorBool.operator()<bool>(true, true);
    testVarValOperatorBool.operator()<bool>(false, false);
}

void writeToFileProxy(const char* content)
{
    std::ofstream ofs("actual.txt");
    ofs << content;
    ofs << std::endl;
};

TEST_F(VarValTest, ostreamTest)
{
    auto testVarValOstream = []<typename T>(const T value)
    {
        using namespace NES::Nautilus;
        VarVal varVal = nautilus::val<T>(value);
        nautilus::stringstream strStreamVarVal;
        std::stringstream strStreamExpected;
        strStreamVarVal << varVal;
        if constexpr (
            std::is_same_v<T, uint8_t> || std::is_same_v<T, int8_t> || std::is_same_v<T, unsigned char> || std::is_same_v<T, char>)
        {
            strStreamExpected << static_cast<int>(value);
        }
        else
        {
            strStreamExpected << value;
        }

        /// Writing the actual and the expected output to a file
        /// We have to do this, as it is not possible to access the underlying data of the nautilus::stringstream object
        nautilus::invoke(writeToFileProxy, strStreamVarVal.str().c_str());
        std::ofstream ofs("expected.txt");
        ofs << strStreamExpected.str() << std::endl;


        /// Comparing the actual and the expected output
        std::ifstream expectedFile("expected.txt");
        std::ifstream actualFile("actual.txt");
        std::string expected((std::istreambuf_iterator<char>(expectedFile)), std::istreambuf_iterator<char>());
        std::string actual((std::istreambuf_iterator<char>(actualFile)), std::istreambuf_iterator<char>());

        EXPECT_EQ(expected, actual);
        return 0;
    };

    testVarValOstream.operator()<int8_t>(-someRandomNumber);
    testVarValOstream.operator()<int16_t>(minI8Minus1);
    testVarValOstream.operator()<int32_t>(minI16Minus1);
    testVarValOstream.operator()<int64_t>(minI32Minus1);
    testVarValOstream.operator()<uint8_t>(someRandomNumber);
    testVarValOstream.operator()<uint16_t>(maxUI8Plus1);
    testVarValOstream.operator()<uint32_t>(maxUI16Plus1);
    testVarValOstream.operator()<uint64_t>(maxUI32Plus1);
    testVarValOstream.operator()<float>(someRandomNumber);
    testVarValOstream.operator()<double>(someRandomNumber);
    testVarValOstream.operator()<bool>(true);
    testVarValOstream.operator()<bool>(false);
}
}
