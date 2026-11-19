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

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nautilus/std/sstream.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

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
        const VarVal varVal = nautilus::val<T>(value);
        EXPECT_EQ(varVal.getRawValueAs<nautilus::val<T>>(), value);
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
        const VarVal varVal = nautilus::val<T>(value);
        const VarVal varValCopy = varVal; /// NOLINT(performance-unnecessary-copy-initialization) - intentional copy for testing
        const VarVal varValMove = std::move(varVal);
        EXPECT_EQ(varValMove.getRawValueAs<nautilus::val<T>>(), value);
        EXPECT_EQ(varValCopy.getRawValueAs<nautilus::val<T>>(), value);
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
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.getRawValueAs<ResultType>(), static_cast<ResultType>(value1 op value2)); \
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
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.getRawValueAs<ResultType>(), static_cast<T>(value1 op value2)); \
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
            if constexpr (requires { nautilus::val<T>(value1) op nautilus::val<T>(value2); }) \
            { \
                const VarVal varVal1 = nautilus::val<T>(value1); \
                const VarVal varVal2 = nautilus::val<T>(value2); \
                const VarVal varValResult = varVal1 op varVal2; \
                using ResultType = decltype(nautilus::val<T>(value1) op nautilus::val<T>(value2)); \
                EXPECT_EQ(varValResult.getRawValueAs<ResultType>(), static_cast<T>(value1 op value2)); \
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
        const VarVal varVal = nautilus::val<T>(value);
        const VarVal result = !varVal;
        EXPECT_EQ(result.getRawValueAs<nautilus::val<bool>>(), !value);
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
    auto testVarValReadFromMemory = []<typename T>(const T value, const DataType& type)
    {
        std::vector<int8_t> memory(sizeof(T));
        std::memcpy(memory.data(), &value, sizeof(T));
        const VarVal varVal = VarVal::readNonNullableVarValFromMemory(memory.data(), type);
        EXPECT_EQ(varVal.getRawValueAs<nautilus::val<T>>(), value);
        return 0;
    };

    testVarValReadFromMemory.operator()<int8_t>(-someRandomNumber, DataType{DataType::Type::INT8, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<int16_t>(minI8Minus1, DataType{DataType::Type::INT16, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<int32_t>(minI16Minus1, DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<int64_t>(minI32Minus1, DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<uint8_t>(someRandomNumber, DataType{DataType::Type::UINT8, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<uint16_t>(maxUI8Plus1, DataType{DataType::Type::UINT16, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<uint32_t>(maxUI16Plus1, DataType{DataType::Type::UINT32, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<uint64_t>(maxUI32Plus1, DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<float>(someRandomNumber, DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<double>(someRandomNumber, DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<bool>(true, DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE});
    testVarValReadFromMemory.operator()<bool>(false, DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE});
}

TEST_F(VarValTest, operatorBoolTest)
{
    auto testVarValOperatorBool = []<typename T>(const T value, const bool expectedValue)
    {
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
    ofs << '\n';
};

TEST_F(VarValTest, ostreamTest)
{
    auto testVarValOstream = []<typename T>(const T value)
    {
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
        std::ofstream ofs{"expected.txt"};
        ofs << strStreamExpected.str() << '\n';
        ofs.flush();

        /// Comparing the actual and the expected output
        std::ifstream expectedFile{"expected.txt"};
        std::ifstream actualFile{"actual.txt"};
        std::string expected{std::istreambuf_iterator<char>{expectedFile}, std::istreambuf_iterator<char>()};
        std::string actual{std::istreambuf_iterator<char>{actualFile}, std::istreambuf_iterator<char>()};

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

TEST_F(VarValTest, testDataTypesChange)
{
    using Types = std::tuple<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;


    /// Implicit data type change should trow an exception if the data types are not equal
    auto testImplicitDataTypeChange = []<typename From, typename To>(From, To)
    {
        /// Checking if the data types are equal / the same
        if constexpr (std::is_same_v<std::decay_t<From>, std::decay_t<From>>)
        {
            const VarVal varValFrom{nautilus::val<From>(42)};
            VarVal varValTo{nautilus::val<To>(43)};
            ASSERT_NO_THROW(varValTo = varValFrom);
        }
        else
        {
            const VarVal varValFrom{nautilus::val<From>(42)};
            VarVal varValTo{nautilus::val<To>(43)};
            ASSERT_EXCEPTION_ERRORCODE(varValTo = varValFrom, ErrorCode::UnknownOperation);
        }
    };

    /// Explicit data type change should work
    auto testExplicitDataTypeChange = []<typename From, typename To>(From, To)
    {
        const VarVal varValFrom{nautilus::val<From>(42)};
        VarVal varValTo{nautilus::val<To>(43)};
        ASSERT_NO_THROW(varValTo = varValFrom);
        ASSERT_EQ(varValTo, varValFrom);
    };


    /// Calls it for every combination of Types
    std::apply([&](auto&&... args) { ((testImplicitDataTypeChange(args, args)), ...); }, std::tuple_cat(Types{}, Types{}));
    std::apply([&](auto&&... args) { ((testExplicitDataTypeChange(args, args)), ...); }, std::tuple_cat(Types{}, Types{}));
}

TEST_F(VarValTest, nullableConstruction)
{
    /// Test non-nullable construction (default)
    const VarVal nonNull(42);
    EXPECT_FALSE(nonNull.isNullable());
    EXPECT_FALSE(static_cast<bool>(nonNull.isNull()));

    /// Test nullable but non-null value
    const VarVal nullable{42, true, nautilus::val<bool>{false}};
    EXPECT_TRUE(nullable.isNullable());
    EXPECT_FALSE(static_cast<bool>(nullable.isNull()));

    /// Test nullable null value
    const VarVal nullValue{0, true, nautilus::val<bool>{true}};
    EXPECT_TRUE(nullValue.isNullable());
    EXPECT_TRUE(static_cast<bool>(nullValue.isNull()));
}

TEST_F(VarValTest, nullPropagationArithmetic)
{
    const VarVal nullVal{nautilus::val<int32_t>(0), true, nautilus::val<bool>{true}};
    const VarVal nonNull{nautilus::val<int32_t>(5)};

    /// NULL + 5 = NULL
    const auto resultAdd = nullVal + nonNull;
    EXPECT_TRUE(resultAdd.isNullable());
    EXPECT_TRUE(static_cast<bool>(resultAdd.isNull()));

    /// 5 + NULL = NULL
    const auto resultAdd2 = nonNull + nullVal;
    EXPECT_TRUE(resultAdd2.isNullable());
    EXPECT_TRUE(static_cast<bool>(resultAdd2.isNull()));

    /// NULL - 5 = NULL
    const auto resultSub = nullVal - nonNull;
    EXPECT_TRUE(static_cast<bool>(resultSub.isNull()));

    /// NULL * 5 = NULL
    const auto resultMul = nullVal * nonNull;
    EXPECT_TRUE(static_cast<bool>(resultMul.isNull()));

    /// NULL / 5 = NULL (using our improved division operator)
    const auto resultDiv = nullVal / nonNull;
    EXPECT_TRUE(static_cast<bool>(resultDiv.isNull()));

    /// 5 / NULL = NULL
    const auto resultDiv2 = nonNull / nullVal;
    EXPECT_TRUE(static_cast<bool>(resultDiv2.isNull()));
}

namespace
{
/// Expects `operation` to fail with an ArithmeticalError (3013) carrying `message` instead of trapping in hardware.
template <typename Operation>
void expectArithmeticalError(const Operation& operation, const std::string_view message)
{
    try
    {
        std::ignore = operation();
        FAIL() << "Expected an ArithmeticalError: " << message;
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::ArithmeticalError);
        EXPECT_NE(std::string_view{exception.what()}.find(message), std::string_view::npos) << exception.what();
    }
}

template <typename Operation>
void expectDivideByZero(const Operation& operation)
{
    expectArithmeticalError(operation, "Can not divide by zero!");
}

template <typename Operation>
void expectModuloByZero(const Operation& operation)
{
    expectArithmeticalError(operation, "Can not modulo by zero!");
}

template <typename Operation>
void expectDivideMinByMinusOne(const Operation& operation)
{
    expectArithmeticalError(operation, "Can not divide MIN by -1!");
}

template <typename Operation>
void expectModuloMinByMinusOne(const Operation& operation)
{
    expectArithmeticalError(operation, "Can not modulo MIN by -1!");
}

constexpr int dividend = 5;

template <typename T>
VarVal notNull(const T value)
{
    return VarVal{nautilus::val<T>(value)};
}

template <typename T>
VarVal nullable(const T value, const bool isNull)
{
    return VarVal{nautilus::val<T>(value), true, nautilus::val<bool>{isNull}};
}
}

TEST_F(VarValTest, divisionByZeroNotNullThrows)
{
    expectDivideByZero([] { return notNull<int8_t>(dividend) / notNull<int8_t>(0); });
    expectDivideByZero([] { return notNull<int32_t>(dividend) / notNull<int32_t>(0); });
    expectDivideByZero([] { return notNull<int64_t>(dividend) / notNull<int64_t>(0); });
    expectDivideByZero([] { return notNull<uint8_t>(dividend) / notNull<uint8_t>(0); });
    expectDivideByZero([] { return notNull<uint64_t>(dividend) / notNull<uint64_t>(0); });
    expectDivideByZero([] { return notNull<int32_t>(dividend) / notNull<uint64_t>(0); });
    expectModuloByZero([] { return notNull<int32_t>(dividend) % notNull<int32_t>(0); });
    expectModuloByZero([] { return notNull<int64_t>(dividend) % notNull<int64_t>(0); });
    expectModuloByZero([] { return notNull<uint8_t>(dividend) % notNull<uint8_t>(0); });
    expectModuloByZero([] { return notNull<uint64_t>(dividend) % notNull<uint64_t>(0); });
    expectModuloByZero([] { return notNull<uint32_t>(dividend) % notNull<int8_t>(0); });
}

TEST_F(VarValTest, divisionByZeroNullableThrows)
{
    expectDivideByZero([] { return nullable<int32_t>(dividend, false) / nullable<int32_t>(0, false); });
    expectDivideByZero([] { return notNull<int32_t>(dividend) / nullable<int32_t>(0, false); });
    expectModuloByZero([] { return nullable<uint64_t>(dividend, false) % notNull<uint64_t>(0); });
    expectModuloByZero([] { return nullable<int8_t>(dividend, false) % nullable<int8_t>(0, false); });
    expectDivideByZero([] { return nullable<uint32_t>(dividend, false) / nullable<uint32_t>(0, false); });
    expectDivideByZero([] { return nullable<uint64_t>(dividend, false) / notNull<uint64_t>(0); });
    expectModuloByZero([] { return nullable<uint32_t>(dividend, false) % nullable<uint32_t>(0, false); });
    expectModuloByZero([] { return notNull<uint64_t>(dividend) % nullable<uint64_t>(0, false); });
    expectDivideByZero([] { return notNull<int16_t>(dividend) / notNull<int16_t>(0); });
    expectModuloByZero([] { return notNull<int16_t>(dividend) % notNull<int16_t>(0); });
    expectDivideByZero([] { return nullable<int16_t>(dividend, false) / nullable<int16_t>(0, false); });
    expectModuloByZero([] { return nullable<int16_t>(dividend, false) % nullable<int16_t>(0, false); });
}

TEST_F(VarValTest, divisionOfNullByNonNullZeroThrows)
{
    expectDivideByZero([] { return nullable<int32_t>(dividend, true) / nullable<int32_t>(0, false); });
    expectModuloByZero([] { return nullable<int32_t>(dividend, true) % notNull<int32_t>(0); });
}

TEST_F(VarValTest, divisionMixedWidthsOfMinByMinusOne)
{
    /// INT32_MIN / INT8(-1) is computed as INT32 and does not fit
    expectDivideMinByMinusOne([] { return notNull<int32_t>(std::numeric_limits<int32_t>::min()) / notNull<int8_t>(-1); });
    expectModuloMinByMinusOne([] { return notNull<int32_t>(std::numeric_limits<int32_t>::min()) % notNull<int8_t>(-1); });
    /// INT32_MIN / INT64(-1) is computed as INT64 and fits
    const auto widened = notNull<int32_t>(std::numeric_limits<int32_t>::min()) / notNull<int64_t>(-1);
    EXPECT_EQ(widened.getRawValueAs<nautilus::val<int64_t>>(), -static_cast<int64_t>(std::numeric_limits<int32_t>::min()));
    /// Nullable INT8: a zero divisor throws, INT8_MIN / -1 is representable after promotion
    expectDivideByZero([] { return nullable<int8_t>(dividend, false) / nullable<int8_t>(0, false); });
    const auto promoted = nullable<int8_t>(std::numeric_limits<int8_t>::min(), false) / nullable<int8_t>(-1, false);
    EXPECT_EQ(promoted.getRawValueAs<nautilus::val<int32_t>>(), int32_t{128});
}

TEST_F(VarValTest, divisionByNullZeroIsNull)
{
    /// A NULL divisor holds arbitrary raw bits (here 0); the result is NULL and must not fail.
    const auto quotient = notNull<int32_t>(dividend) / nullable<int32_t>(0, true);
    EXPECT_TRUE(static_cast<bool>(quotient.isNull()));
    const auto remainder = nullable<int32_t>(dividend, false) % nullable<int32_t>(0, true);
    EXPECT_TRUE(static_cast<bool>(remainder.isNull()));
}

TEST_F(VarValTest, divisionOfMinByMinusOneThrows)
{
    expectDivideMinByMinusOne([] { return notNull<int32_t>(std::numeric_limits<int32_t>::min()) / notNull<int32_t>(-1); });
    expectDivideMinByMinusOne([] { return notNull<int64_t>(std::numeric_limits<int64_t>::min()) / notNull<int64_t>(-1); });
    expectModuloMinByMinusOne([] { return notNull<int32_t>(std::numeric_limits<int32_t>::min()) % notNull<int32_t>(-1); });
    expectModuloMinByMinusOne([] { return notNull<int64_t>(std::numeric_limits<int64_t>::min()) % notNull<int64_t>(-1); });
    expectDivideMinByMinusOne([] { return nullable<int64_t>(std::numeric_limits<int64_t>::min(), false) / nullable<int64_t>(-1, false); });
    expectModuloMinByMinusOne([] { return nullable<int32_t>(std::numeric_limits<int32_t>::min(), false) % notNull<int32_t>(-1); });
    expectModuloMinByMinusOne([] { return nullable<int64_t>(std::numeric_limits<int64_t>::min(), false) % nullable<int64_t>(-1, false); });
}

TEST_F(VarValTest, divisionOfNullMinByMinusOneIsNull)
{
    /// The raw bits of a NULL operand can be MIN / -1; the discarded division must not trap and the result is NULL.
    const auto quotient = nullable<int64_t>(std::numeric_limits<int64_t>::min(), true) / notNull<int64_t>(-1);
    EXPECT_TRUE(static_cast<bool>(quotient.isNull()));
    const auto remainder = nullable<int32_t>(std::numeric_limits<int32_t>::min(), true) % nullable<int32_t>(-1, false);
    EXPECT_TRUE(static_cast<bool>(remainder.isNull()));
}

TEST_F(VarValTest, divisionNeighboursOfGuardedCasesStillWork)
{
    constexpr auto minI64 = std::numeric_limits<int64_t>::min();
    /// INT8_MIN / -1 and INT16_MIN / -1 are promoted to int and are representable
    const auto promotedI8 = notNull<int8_t>(std::numeric_limits<int8_t>::min()) / notNull<int8_t>(-1);
    EXPECT_EQ(promotedI8.getRawValueAs<nautilus::val<int32_t>>(), int32_t{128});
    const auto promotedI16 = notNull<int16_t>(std::numeric_limits<int16_t>::min()) / notNull<int16_t>(-1);
    EXPECT_EQ(promotedI16.getRawValueAs<nautilus::val<int32_t>>(), int32_t{32768});
    const auto promotedI16Remainder = notNull<int16_t>(std::numeric_limits<int16_t>::min()) % notNull<int16_t>(-1);
    EXPECT_EQ(promotedI16Remainder.getRawValueAs<nautilus::val<int32_t>>(), int32_t{0});
    const auto nullableI16 = nullable<int16_t>(std::numeric_limits<int16_t>::min(), false) / nullable<int16_t>(-1, false);
    EXPECT_EQ(nullableI16.getRawValueAs<nautilus::val<int32_t>>(), int32_t{32768});
    const auto nullableI16Remainder = nullable<int16_t>(std::numeric_limits<int16_t>::min(), false) % nullable<int16_t>(-1, false);
    EXPECT_EQ(nullableI16Remainder.getRawValueAs<nautilus::val<int32_t>>(), int32_t{0});
    const auto minByOne = notNull<int64_t>(minI64) / notNull<int64_t>(1);
    EXPECT_EQ(minByOne.getRawValueAs<nautilus::val<int64_t>>(), minI64);
    const auto plusOne = notNull<int64_t>(minI64 + 1) / notNull<int64_t>(-1);
    EXPECT_EQ(plusOne.getRawValueAs<nautilus::val<int64_t>>(), std::numeric_limits<int64_t>::max());
    const auto remainder = notNull<int64_t>(minI64) % notNull<int64_t>(2);
    EXPECT_EQ(remainder.getRawValueAs<nautilus::val<int64_t>>(), 0);
    const auto unsignedQuotient = notNull<uint64_t>(std::numeric_limits<uint64_t>::max()) / notNull<uint64_t>(1);
    EXPECT_EQ(unsignedQuotient.getRawValueAs<nautilus::val<uint64_t>>(), std::numeric_limits<uint64_t>::max());
    const auto nullableQuotient = nullable<int32_t>(-9, false) / nullable<int32_t>(3, false);
    EXPECT_EQ(nullableQuotient.getRawValueAs<nautilus::val<int32_t>>(), int32_t{-3});
    EXPECT_FALSE(static_cast<bool>(nullableQuotient.isNull()));
}

TEST_F(VarValTest, floatingPointDivisionByZeroNotNullIsUnchanged)
{
    constexpr auto infinity = std::numeric_limits<double>::infinity();
    const auto positiveInfinity = notNull<double>(5.0) / notNull<double>(0.0);
    EXPECT_TRUE(static_cast<bool>(positiveInfinity.getRawValueAs<nautilus::val<double>>() == infinity));
    const auto negativeInfinity = notNull<double>(-5.0) / notNull<double>(0.0);
    EXPECT_TRUE(static_cast<bool>(negativeInfinity.getRawValueAs<nautilus::val<double>>() == -infinity));
    const auto notANumber = notNull<double>(0.0) / notNull<double>(0.0);
    const auto notANumberValue = notANumber.getRawValueAs<nautilus::val<double>>();
    EXPECT_TRUE(static_cast<bool>(notANumberValue != notANumberValue));
    const auto floatInfinity = notNull<float>(5.0F) / notNull<float>(0.0F);
    EXPECT_TRUE(static_cast<bool>(floatInfinity.getRawValueAs<nautilus::val<float>>() == std::numeric_limits<float>::infinity()));
    EXPECT_FALSE(positiveInfinity.isNullable());
}

TEST_F(VarValTest, nullPropagationComparison)
{
    const VarVal nullVal{nautilus::val<int32_t>(0), true, nautilus::val<bool>{true}};
    const VarVal nonNull{nautilus::val<int32_t>(5)};

    /// NULL == 5 → NULL
    const auto resultEq = nullVal == nonNull;
    EXPECT_TRUE(static_cast<bool>(resultEq.isNull()));

    /// NULL != 5 → NULL
    const auto resultNeq = nullVal != nonNull;
    EXPECT_TRUE(static_cast<bool>(resultNeq.isNull()));

    /// NULL < 5 → NULL
    const auto resultLt = nullVal < nonNull;
    EXPECT_TRUE(static_cast<bool>(resultLt.isNull()));

    /// NULL > 5 → NULL
    const auto resultGt = nullVal > nonNull;
    EXPECT_TRUE(static_cast<bool>(resultGt.isNull()));
}

TEST_F(VarValTest, nullBooleanConversion)
{
    /// Non-null true in non-nullable context
    const VarVal trueNonNullable{nautilus::val<bool>(true), false, nautilus::val<bool>{false}};
    EXPECT_TRUE(static_cast<bool>(trueNonNullable));

    /// Non-null false in non-nullable context
    const VarVal falseNonNullable{nautilus::val<bool>(false), false, nautilus::val<bool>{false}};
    EXPECT_FALSE(static_cast<bool>(falseNonNullable));

    /// NULL in nullable context, but with different underlying value (should still be false)
    const VarVal nullValWithTrue{nautilus::val<bool>(false), true, nautilus::val<bool>{true}};
    EXPECT_FALSE(static_cast<bool>(nullValWithTrue));

    /// NULL in nullable context, but with different underlying value (should still be false)
    const VarVal nullValWithFalse{nautilus::val<bool>(true), true, nautilus::val<bool>{true}};
    EXPECT_FALSE(static_cast<bool>(nullValWithFalse));

    /// Copy constructor/assignment: NULL remains NULL
    const VarVal nullValOriginal{nautilus::val<bool>(true), true, nautilus::val<bool>{true}};
    VarVal nullValCopy = nullValOriginal;
    EXPECT_FALSE(static_cast<bool>(nullValCopy));

    /// Move constructor/assignment: NULL remains NULL
    const VarVal nullValMoved = std::move(nullValCopy);
    EXPECT_FALSE(static_cast<bool>(nullValMoved));
}

TEST_F(VarValTest, booleanComparisonOperators)
{
    /// NULL value
    const VarVal nullVal{nautilus::val<bool>(true), true, nautilus::val<bool>{true}};
    /// Non-null true value
    const VarVal trueVal{nautilus::val<bool>(true), true, nautilus::val<bool>{false}};
    const VarVal trueVal2{nautilus::val<bool>(true), true, nautilus::val<bool>{false}};
    /// Non-null false value
    const VarVal falseVal{nautilus::val<bool>(false), true, nautilus::val<bool>{false}};
    const VarVal falseVal2{nautilus::val<bool>(false), true, nautilus::val<bool>{false}};

    /// NULL == true -> NULL (evaluates to false in boolean context)
    const auto eqResult1 = nullVal == trueVal;
    EXPECT_FALSE(static_cast<bool>(eqResult1));

    /// NULL != true -> NULL (evaluates to false in boolean context, or true if your logic inverts NULL comparisons)
    const auto neqResult1 = nullVal != trueVal;
    /// Adjust expectation based on your logic for NULL != non-NULL
    EXPECT_FALSE(static_cast<bool>(neqResult1));

    /// true == true -> true
    const auto eqResult2 = trueVal == trueVal2;
    EXPECT_TRUE(static_cast<bool>(eqResult2));

    /// true != false -> true
    const auto neqResult2 = trueVal != falseVal;
    EXPECT_TRUE(static_cast<bool>(neqResult2));

    /// false == false -> true
    const auto eqResult3 = falseVal == falseVal2;
    EXPECT_TRUE(static_cast<bool>(eqResult3));
}


}
