//
// Created by dbpro4 on 10/23/22.
//

#include </home/dbpro4/nebulastream/nes-runtime/tests/include/TestUtils/ExpressionWrapper.hpp>
#include </home/dbpro4/nebulastream/nes-runtime/include/Execution/Expressions/Functions/BitcounterExpression.hpp>
#include </home/dbpro4/nebulastream/nes-common/include/Util/Logger/Logger.hpp>
#include </home/dbpro4/nebulastream/cmake-build-debug/nes-dependencies-v20-x64-linux-nes/installed/x64-linux-nes/include/gtest/gtest.h>
#include </home/dbpro4/nebulastream/nes-runtime/include/Execution/Expressions/Expression.hpp>
#include </usr/include/c++/9/memory>



namespace NES::Runtime::Execution::Expressions {


class BitcounterExpressionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BitcounterExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup BitcounterExpressionTest test class." << std::endl;
    }
    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};
TEST_F(BitcounterExpressionTest, divIntegers) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 31));
        ASSERT_EQ(resultValue, 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 31));
        ASSERT_EQ(resultValue, 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(31));
        ASSERT_EQ(resultValue, 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 31));
        ASSERT_EQ(resultValue, 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
}
}

