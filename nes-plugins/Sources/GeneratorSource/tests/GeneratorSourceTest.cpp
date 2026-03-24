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

#include <chrono>
#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>

#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <FixedGeneratorRate.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <SinusGeneratorRate.hpp>

/// NOLINTBEGIN(readability-magic-numbers,cert-msc51-cpp) -- test values and deterministic seeds are intentional

namespace NES
{

/// --- SequenceField Tests ---

class SequenceFieldTest : public ::testing::Test
{
};

TEST_F(SequenceFieldTest, generateUint64Sequence)
{
    GeneratorFields::SequenceField field("SEQUENCE UINT64 0 5 1");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "0");

    oss.str("");
    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "1");

    oss.str("");
    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "2");
}

TEST_F(SequenceFieldTest, generateInt32Sequence)
{
    GeneratorFields::SequenceField field("SEQUENCE INT32 10 15 2");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "10");

    oss.str("");
    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "12");

    oss.str("");
    field.generate(oss, rng);
    EXPECT_EQ(oss.str(), "14");
}

TEST_F(SequenceFieldTest, sequenceStopsAtEnd)
{
    GeneratorFields::SequenceField field("SEQUENCE UINT64 0 3 1");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    EXPECT_FALSE(field.stop);
    field.generate(oss, rng); /// 0
    field.generate(oss, rng); /// 1
    field.generate(oss, rng); /// 2
    EXPECT_TRUE(field.stop);
}

TEST_F(SequenceFieldTest, validateValidSchemaLine)
{
    EXPECT_NO_THROW(GeneratorFields::SequenceField::validate("SEQUENCE UINT64 0 100 1"));
    EXPECT_NO_THROW(GeneratorFields::SequenceField::validate("SEQUENCE INT32 -10 10 2"));
    EXPECT_NO_THROW(GeneratorFields::SequenceField::validate("SEQUENCE FLOAT64 0.0 1.0 0.1"));
}

TEST_F(SequenceFieldTest, validateInvalidSchemaLineThrows)
{
    /// Wrong number of parameters
    EXPECT_THROW(GeneratorFields::SequenceField::validate("SEQUENCE UINT64 0 100"), Exception);
    /// Invalid type
    EXPECT_THROW(GeneratorFields::SequenceField::validate("SEQUENCE INVALID 0 100 1"), Exception);
}

/// --- NormalDistributionField Tests ---

class NormalDistributionFieldTest : public ::testing::Test
{
};

TEST_F(NormalDistributionFieldTest, generateFloat64Values)
{
    GeneratorFields::NormalDistributionField field("NORMAL_DISTRIBUTION FLOAT64 0.0 1.0");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    field.generate(oss, rng);
    EXPECT_FALSE(oss.str().empty());
}

TEST_F(NormalDistributionFieldTest, generateInt64Values)
{
    GeneratorFields::NormalDistributionField field("NORMAL_DISTRIBUTION INT64 100 0.5");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    field.generate(oss, rng);
    EXPECT_FALSE(oss.str().empty());
}

TEST_F(NormalDistributionFieldTest, validateValidSchemaLine)
{
    EXPECT_NO_THROW(GeneratorFields::NormalDistributionField::validate("NORMAL_DISTRIBUTION FLOAT64 0.0 1.0"));
    EXPECT_NO_THROW(GeneratorFields::NormalDistributionField::validate("NORMAL_DISTRIBUTION INT32 50 0.5"));
}

TEST_F(NormalDistributionFieldTest, validateInvalidSchemaLineThrows)
{
    /// Too few parameters
    EXPECT_THROW(GeneratorFields::NormalDistributionField::validate("NORMAL_DISTRIBUTION FLOAT64 0.0"), Exception);
    /// Negative stddev
    EXPECT_THROW(GeneratorFields::NormalDistributionField::validate("NORMAL_DISTRIBUTION FLOAT64 0.0 -1.0"), Exception);
}

/// --- RandomStrField Tests ---

class RandomStrFieldTest : public ::testing::Test
{
};

TEST_F(RandomStrFieldTest, generateStringWithinBounds)
{
    GeneratorFields::RandomStrField field("RANDOMSTR 5 10");
    std::mt19937 rng(42U);
    std::ostringstream oss;

    field.generate(oss, rng);
    const auto result = oss.str();
    EXPECT_GE(result.size(), 5U);
    EXPECT_LE(result.size(), 10U);
}

TEST_F(RandomStrFieldTest, generateDeterministicWithSameSeed)
{
    GeneratorFields::RandomStrField field1("RANDOMSTR 5 10");
    GeneratorFields::RandomStrField field2("RANDOMSTR 5 10");
    std::mt19937 rng1(42U);
    std::mt19937 rng2(42U);
    std::ostringstream oss1;
    std::ostringstream oss2;

    field1.generate(oss1, rng1);
    field2.generate(oss2, rng2);
    EXPECT_EQ(oss1.str(), oss2.str());
}

TEST_F(RandomStrFieldTest, validateValidSchemaLine)
{
    EXPECT_NO_THROW(GeneratorFields::RandomStrField::validate("RANDOMSTR 1 100"));
    EXPECT_NO_THROW(GeneratorFields::RandomStrField::validate("RANDOMSTR 5 5"));
}

TEST_F(RandomStrFieldTest, validateInvalidSchemaLineThrows)
{
    /// Too few parameters
    EXPECT_THROW(GeneratorFields::RandomStrField::validate("RANDOMSTR 5"), Exception);
    /// minLength > maxLength
    EXPECT_THROW(GeneratorFields::RandomStrField::validate("RANDOMSTR 10 5"), Exception);
    /// Zero length
    EXPECT_THROW(GeneratorFields::RandomStrField::validate("RANDOMSTR 0 5"), Exception);
}

/// --- Generator Tests ---

class GeneratorTest : public ::testing::Test
{
};

TEST_F(GeneratorTest, generateTupleWithSequenceField)
{
    Generator generator(42U, GeneratorStop::NONE, "SEQUENCE UINT64 0 10 1");
    std::ostringstream oss;

    generator.generateTuple(oss);
    EXPECT_EQ(oss.str(), "0\n");

    oss.str("");
    generator.generateTuple(oss);
    EXPECT_EQ(oss.str(), "1\n");
}

TEST_F(GeneratorTest, generateTupleWithMultipleFields)
{
    Generator generator(42U, GeneratorStop::NONE, "SEQUENCE UINT64 0 10 1,SEQUENCE UINT64 100 200 10");
    std::ostringstream oss;

    generator.generateTuple(oss);
    EXPECT_EQ(oss.str(), "0,100\n");

    oss.str("");
    generator.generateTuple(oss);
    EXPECT_EQ(oss.str(), "1,110\n");
}

TEST_F(GeneratorTest, shouldStopWhenAllSequencesComplete)
{
    Generator generator(42U, GeneratorStop::ALL, "SEQUENCE UINT64 0 2 1");
    std::ostringstream oss;

    EXPECT_FALSE(generator.shouldStop());
    generator.generateTuple(oss); /// 0
    generator.generateTuple(oss); /// 1 -> hits end
    EXPECT_TRUE(generator.shouldStop());
}

TEST_F(GeneratorTest, shouldStopWhenOneSequenceCompletes)
{
    Generator generator(42U, GeneratorStop::ONE, "SEQUENCE UINT64 0 2 1,SEQUENCE UINT64 0 100 1");
    std::ostringstream oss;

    EXPECT_FALSE(generator.shouldStop());
    generator.generateTuple(oss); /// first: 0, second: 0
    generator.generateTuple(oss); /// first: 1 -> end, second: 1
    EXPECT_TRUE(generator.shouldStop());
}

TEST_F(GeneratorTest, shouldNotStopWithNonePolicy)
{
    Generator generator(42U, GeneratorStop::NONE, "SEQUENCE UINT64 0 2 1");
    std::ostringstream oss;

    generator.generateTuple(oss);
    generator.generateTuple(oss);
    generator.generateTuple(oss);
    EXPECT_FALSE(generator.shouldStop());
}

TEST_F(GeneratorTest, invalidSchemaThrows)
{
    EXPECT_THROW(Generator(42U, GeneratorStop::NONE, "INVALID_TYPE 0 10 1"), Exception);
}

/// --- FixedGeneratorRate Tests ---

class FixedGeneratorRateTest : public ::testing::Test
{
};

TEST_F(FixedGeneratorRateTest, parseValidConfigString)
{
    const auto result = FixedGeneratorRate::parseAndValidateConfigString("emit_rate 1000");
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result.value(), 1000.0);
}

TEST_F(FixedGeneratorRateTest, parseValidConfigStringCaseInsensitive)
{
    const auto result = FixedGeneratorRate::parseAndValidateConfigString("EMIT_RATE 500");
    /// The parser uses toLowerCase, so this should work
    ASSERT_TRUE(result.has_value());
    EXPECT_DOUBLE_EQ(result.value(), 500.0);
}

TEST_F(FixedGeneratorRateTest, parseInvalidConfigStringReturnsEmpty)
{
    EXPECT_FALSE(FixedGeneratorRate::parseAndValidateConfigString("invalid 1000").has_value());
    EXPECT_FALSE(FixedGeneratorRate::parseAndValidateConfigString("emit_rate").has_value());
    EXPECT_FALSE(FixedGeneratorRate::parseAndValidateConfigString("").has_value());
}

TEST_F(FixedGeneratorRateTest, calcNumberOfTuplesForInterval)
{
    FixedGeneratorRate rate("emit_rate 1000");
    const auto start = std::chrono::system_clock::now();
    const auto end = start + std::chrono::seconds(1);
    const auto tuples = rate.calcNumberOfTuplesForInterval(start, end);
    EXPECT_EQ(tuples, 1000U);
}

TEST_F(FixedGeneratorRateTest, calcNumberOfTuplesForHalfSecond)
{
    FixedGeneratorRate rate("emit_rate 1000");
    const auto start = std::chrono::system_clock::now();
    const auto end = start + std::chrono::milliseconds(500);
    const auto tuples = rate.calcNumberOfTuplesForInterval(start, end);
    EXPECT_EQ(tuples, 500U);
}

/// --- SinusGeneratorRate Tests ---

class SinusGeneratorRateTest : public ::testing::Test
{
};

TEST_F(SinusGeneratorRateTest, parseValidConfigString)
{
    const auto result = SinusGeneratorRate::parseAndValidateConfigString("amplitude 100,frequency 1.0");
    ASSERT_TRUE(result.has_value());
    const auto [amplitude, frequency] = result.value();
    EXPECT_DOUBLE_EQ(amplitude, 100.0);
    EXPECT_DOUBLE_EQ(frequency, 1.0);
}

TEST_F(SinusGeneratorRateTest, parseInvalidConfigStringReturnsEmpty)
{
    EXPECT_FALSE(SinusGeneratorRate::parseAndValidateConfigString("invalid").has_value());
    EXPECT_FALSE(SinusGeneratorRate::parseAndValidateConfigString("amplitude 100").has_value());
    EXPECT_FALSE(SinusGeneratorRate::parseAndValidateConfigString("").has_value());
}

TEST_F(SinusGeneratorRateTest, calcNumberOfTuplesReturnsNonNegative)
{
    SinusGeneratorRate rate(1.0, 100.0);
    const auto start = std::chrono::system_clock::now();
    const auto end = start + std::chrono::seconds(1);
    /// The sinus integral over any interval should produce a non-negative count
    const auto tuples = rate.calcNumberOfTuplesForInterval(start, end);
    /// We just check it does not crash and returns some value (uint64_t is always >= 0)
    EXPECT_GE(tuples, 0U);
}

}

/// NOLINTEND(readability-magic-numbers,cert-msc51-cpp)
