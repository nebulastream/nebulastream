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

#include <cstring>
#include <iosfwd>
#include <random>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/iostream.h>
#include <nautilus/std/sstream.h>
#include <BaseUnitTest.hpp>

namespace NES
{


class VariableSizedDataTest : public Testing::BaseUnitTest
{
public:
    static constexpr auto sizeOfLengthInBytes = 4;

    static void SetUpTestCase()
    {
        Logger::setupLogging("VariableSizedDataTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup VariableSizedDataTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down VariableSizedDataTest class."); }

    static std::vector<int8_t> createVariableSizedRandomData(uint32_t size)
    {
        std::vector<int8_t> content(sizeOfLengthInBytes + size);
        for (uint32_t i = 0; i < size; i++)
        {
            content[sizeOfLengthInBytes + i] = rand() % 256;
        }

        std::memcpy(content.data(), &size, sizeOfLengthInBytes);
        return content;
    }
};

TEST_F(VariableSizedDataTest, SimpleConstruction)
{
    {
        /// Testing creating a variable sized data object from a pointer
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};
        EXPECT_EQ(varSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                varSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }

    {
        /// Testing creating a variable sized data object from a pointer and a size
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};
        EXPECT_EQ(varSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                varSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }
}

TEST_F(VariableSizedDataTest, CopyConstruction)
{
    using namespace NES::Nautilus;
    constexpr auto sizeInBytes = 1024;
    auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
    const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

    /// Test, if we can copy the variable sized data object by copy operator=
    const VarVal copiedVarSizedData = varSizedData;
    EXPECT_EQ(copiedVarSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
    EXPECT_TRUE(
        std::memcmp(
            copiedVarSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
        == 0);

    /// Test, if we can copy the variable sized data object by copy constructor
    const VarVal copiedVarSizedData2(varSizedData);
    EXPECT_EQ(copiedVarSizedData2.cast<VariableSizedData>().getContentSize(), sizeInBytes);
    EXPECT_TRUE(
        std::memcmp(
            copiedVarSizedData2.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
        == 0);
}

TEST_F(VariableSizedDataTest, MoveConstruction)
{
    {
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

        /// Test, if we can move the variable sized data object by move operator=
        const VarVal movedVarSizedData = std::move(varSizedData);
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                movedVarSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }

    {
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

        /// Test, if we can move the variable sized data object by move constructor
        const VarVal movedVarSizedData(std::move(varSizedData));
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                movedVarSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }
}

TEST_F(VariableSizedDataTest, AssignmentConstruction)
{
    {
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData = VarVal(VariableSizedData(ptrToVariableSized));

        /// Test, if we can move the variable sized data object by move operator=
        const VarVal movedVarSizedData = std::move(varSizedData);
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                movedVarSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }

    {
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

        /// Test, if we can move the variable sized data object by move constructor
        const VarVal movedVarSizedData(std::move(varSizedData));
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getContentSize(), sizeInBytes);
        EXPECT_TRUE(
            std::memcmp(
                movedVarSizedData.cast<VariableSizedData>().getContent().value, variableSizedData.data() + sizeOfLengthInBytes, sizeInBytes)
            == 0);
    }
}

TEST_F(VariableSizedDataTest, binaryOperatorOverloads)
{
    {
        /// Testing if exact same variable sized data objects are equal
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

        const VarVal copiedVarSizedData = varSizedData;
        EXPECT_TRUE(copiedVarSizedData.cast<VariableSizedData>() == varSizedData.cast<VariableSizedData>());
        EXPECT_FALSE(copiedVarSizedData.cast<VariableSizedData>() != varSizedData.cast<VariableSizedData>());
    }

    {
        /// Testing if two variable sized data with different sizes but same content are not equal
        using namespace NES::Nautilus;
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        auto variableSizedDataDouble = createVariableSizedRandomData(sizeInBytes + sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const nautilus::val<int8_t*> ptrToVariableSizedDouble(variableSizedDataDouble.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};
        const VarVal varSizedDataDouble{VariableSizedData(ptrToVariableSizedDouble)};
        EXPECT_FALSE(varSizedData.cast<VariableSizedData>() == varSizedDataDouble.cast<VariableSizedData>());
        EXPECT_TRUE(varSizedData.cast<VariableSizedData>() != varSizedDataDouble.cast<VariableSizedData>());
    }

    {
        /// Testing if two variables sized data with the same size but in a randomize fashion. To ensure deterministic results, we
        /// print the seed of the random number generator to be able to reproduce the results.
        const auto seed = std::random_device()();
        NES_INFO("Seed: {}", seed);
        std::srand(seed);

        using namespace NES::Nautilus;
        constexpr auto maxSize = 5; /// We set the size quite small to ensure that we also happen to have the same content
        const auto sizeInBytes = (std::rand() % maxSize) + 1;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        auto otherVariableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const nautilus::val<int8_t*> ptrToOtherVariableSized(otherVariableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};
        const VarVal otherVarSizedData{VariableSizedData(ptrToOtherVariableSized)};

        const bool isEqual = variableSizedData == otherVariableSizedData;
        EXPECT_EQ(isEqual, varSizedData.cast<VariableSizedData>() == otherVarSizedData.cast<VariableSizedData>());
    }
}

void compareStringProxy(const char* actual, const char* expected)
{
    const std::string actualStr(actual);
    const std::string expectedStr(expected);
    EXPECT_EQ(expectedStr, actualStr);
}

TEST_F(VariableSizedDataTest, ostreamTest)
{
    using namespace NES::Nautilus;
    constexpr auto sizeInBytes = 1024;
    auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
    const VarVal varSizedData{VariableSizedData(ptrToVariableSized)};

    /// Comparing the output of the ostream operator with the expected output
    std::stringstream expectedOutput;
    expectedOutput << "Size(" << sizeInBytes << "): ";
    for (uint32_t i = 0; i < sizeInBytes; ++i)
    {
        expectedOutput << std::hex << static_cast<int>(variableSizedData[sizeOfLengthInBytes + i] & 0xff) << " ";
    }
    nautilus::stringstream expected;
    expected << expectedOutput.str().c_str();

    nautilus::stringstream output;
    output << varSizedData;

    /// Performing the comparison in a proxy function, as we cannot access the underlying data of the nautilus::stringstream object
    nautilus::invoke(compareStringProxy, output.str().c_str(), expected.str().c_str());
}

}
