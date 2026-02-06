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
#include <cstdlib>
#include <cstring>
#include <ios>
#include <iosfwd>
#include <memory>
#include <random>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nautilus/function.hpp>
#include <nautilus/std/sstream.h>
#include <std/cstring.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <val_ptr.hpp>

namespace NES
{


class VariableSizedDataTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("VariableSizedDataTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup VariableSizedDataTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down VariableSizedDataTest class."); }

    static std::vector<int8_t> createVariableSizedRandomData(uint32_t size)
    {
        std::vector<int8_t> content(size);
        for (uint32_t i = 0; i < size; i++)
        {
            content[i] = rand() % 256;
        }

        return content;
    }
};

TEST_F(VariableSizedDataTest, SimpleConstruction)
{
    {
        /// Testing creating a variable sized data object from a pointer
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};
        EXPECT_EQ(varSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(nautilus::memcmp(varSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
    }

    {
        /// Testing creating a variable sized data object from a pointer and a size
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};
        EXPECT_EQ(varSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(nautilus::memcmp(varSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
    }
}

TEST_F(VariableSizedDataTest, CopyConstruction)
{
    constexpr auto sizeInBytes = 1024;
    auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
    const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};

    /// Test, if we can copy the variable sized data object by copy operator=
    const VarVal copiedVarSizedData = varSizedData;
    EXPECT_EQ(copiedVarSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
    EXPECT_TRUE(nautilus::memcmp(copiedVarSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);

    /// Test, if we can copy the variable sized data object by copy constructor
    const VarVal copiedVarSizedData2(varSizedData);
    EXPECT_EQ(copiedVarSizedData2.cast<VariableSizedData>().getSize(), sizeInBytes);
    EXPECT_TRUE(nautilus::memcmp(copiedVarSizedData2.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
}

TEST_F(VariableSizedDataTest, MoveConstruction)
{
    {
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};

        /// Test, if we can move the variable sized data object by move operator=
        const VarVal movedVarSizedData = std::move(varSizedData);
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(nautilus::memcmp(movedVarSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
    }

    {
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, sizeInBytes)};

        /// Test, if we can move the variable sized data object by move constructor
        const VarVal movedVarSizedData(std::move(varSizedData));
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(nautilus::memcmp(movedVarSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
    }
}

TEST_F(VariableSizedDataTest, AssignmentConstruction)
{
    {
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData = VarVal(VariableSizedData(ptrToVariableSized, sizeInBytes));

        /// Test, if we can move the variable sized data object by move operator=
        const VarVal movedVarSizedData = std::move(varSizedData);
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(nautilus::memcmp(movedVarSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), sizeInBytes) == 0);
    }

    {
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, variableSizedData.size())};

        /// Test, if we can move the variable sized data object by move constructor
        const VarVal movedVarSizedData(std::move(varSizedData));
        EXPECT_EQ(movedVarSizedData.cast<VariableSizedData>().getSize(), sizeInBytes);
        EXPECT_TRUE(
            nautilus::memcmp(movedVarSizedData.cast<VariableSizedData>().getContent(), variableSizedData.data(), variableSizedData.size())
            == 0);
    }
}

TEST_F(VariableSizedDataTest, binaryOperatorOverloads)
{
    {
        /// Testing if exact same variable sized data objects are equal
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, variableSizedData.size())};

        const VarVal copiedVarSizedData = varSizedData;
        EXPECT_TRUE(copiedVarSizedData.cast<VariableSizedData>() == varSizedData.cast<VariableSizedData>());
        EXPECT_FALSE(copiedVarSizedData.cast<VariableSizedData>() != varSizedData.cast<VariableSizedData>());
    }

    {
        /// Testing if two variable sized data with different sizes but same content are not equal
        constexpr auto sizeInBytes = 1024;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        auto variableSizedDataDouble = createVariableSizedRandomData(sizeInBytes + sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const nautilus::val<int8_t*> ptrToVariableSizedDouble(variableSizedDataDouble.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, variableSizedData.size())};
        const VarVal varSizedDataDouble{VariableSizedData(ptrToVariableSizedDouble, variableSizedDataDouble.size())};
        EXPECT_FALSE(varSizedData.cast<VariableSizedData>() == varSizedDataDouble.cast<VariableSizedData>());
        EXPECT_TRUE(varSizedData.cast<VariableSizedData>() != varSizedDataDouble.cast<VariableSizedData>());
    }

    {
        /// Testing if two variables sized data with the same size but in a randomize fashion. To ensure deterministic results, we
        /// print the seed of the random number generator to be able to reproduce the results.
        const auto seed = std::random_device()();
        NES_INFO("Seed: {}", seed);
        std::srand(seed);

        constexpr auto maxSize = 5; /// We set the size quite small to ensure that we also happen to have the same content
        const auto sizeInBytes = (std::rand() % maxSize) + 1;
        auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
        auto otherVariableSizedData = createVariableSizedRandomData(sizeInBytes);
        const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
        const nautilus::val<int8_t*> ptrToOtherVariableSized(otherVariableSizedData.data());
        const VarVal varSizedData{VariableSizedData(ptrToVariableSized, variableSizedData.size())};
        const VarVal otherVarSizedData{VariableSizedData(ptrToOtherVariableSized, otherVariableSizedData.size())};

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
    constexpr auto sizeInBytes = 1024;
    auto variableSizedData = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptrToVariableSized(variableSizedData.data());
    const VarVal varSizedData{VariableSizedData(ptrToVariableSized, variableSizedData.size())};

    /// Comparing the output of the ostream operator with the expected output
    std::stringstream expectedOutput;
    expectedOutput << "Size(" << sizeInBytes << "): ";
    for (uint32_t i = 0; i < sizeInBytes; ++i)
    {
        expectedOutput << std::hex << static_cast<int>(variableSizedData[i] & 0xff) << " ";
    }
    nautilus::stringstream expected;
    expected << expectedOutput.str().c_str();

    nautilus::stringstream output;
    output << varSizedData;

    /// Performing the comparison in a proxy function, as we cannot access the underlying data of the nautilus::stringstream object
    nautilus::invoke(compareStringProxy, output.str().c_str(), expected.str().c_str());
}

TEST_F(VariableSizedDataTest, IsValid)
{
    constexpr auto sizeInBytes = 512;
    auto data = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptr(data.data());

    const VariableSizedData varData(ptr, sizeInBytes);
    EXPECT_TRUE(varData.isValid());
}

TEST_F(VariableSizedDataTest, GetSizeAndContent)
{
    constexpr auto sizeInBytes = 512;
    auto data = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptr(data.data());

    const VariableSizedData varData(ptr, sizeInBytes);

    EXPECT_EQ(varData.getSize(), sizeInBytes);
    EXPECT_TRUE(nautilus::memcmp(varData.getContent(), ptr, sizeInBytes) == 0);
}

TEST_F(VariableSizedDataTest, EqualityWithSameData)
{
    constexpr auto sizeInBytes = 512;
    auto data1 = createVariableSizedRandomData(sizeInBytes);
    auto data2 = data1; // Copy same data

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, sizeInBytes);
    const VariableSizedData varData2(ptr2, sizeInBytes);

    EXPECT_TRUE(varData1 == varData2);
    EXPECT_FALSE(varData1 != varData2);
}

TEST_F(VariableSizedDataTest, InequalityWithDifferentData)
{
    constexpr auto sizeInBytes = 512;
    auto data1 = createVariableSizedRandomData(sizeInBytes);
    auto data2 = createVariableSizedRandomData(sizeInBytes);
    data2[0] = static_cast<int8_t>(data1[0] + 1); // Ensure different

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, sizeInBytes);
    const VariableSizedData varData2(ptr2, sizeInBytes);

    EXPECT_FALSE(varData1 == varData2);
    EXPECT_TRUE(varData1 != varData2);
}

TEST_F(VariableSizedDataTest, InequalityWithDifferentSizes)
{
    constexpr auto size1 = 512;
    constexpr auto size2 = 256;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);

    EXPECT_FALSE(varData1 == varData2);
    EXPECT_TRUE(varData1 != varData2);
}

TEST_F(VariableSizedDataTest, GetReference)
{
    constexpr auto sizeInBytes = 512;
    auto data = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptr(data.data());

    const VariableSizedData varData(ptr, sizeInBytes);

    // getReference should return same as getContent for regular data
    EXPECT_TRUE(nautilus::memcmp(varData.getReference(), varData.getContent(), sizeInBytes) == 0);
}

TEST_F(VariableSizedDataTest, SmallDataSize)
{
    constexpr auto sizeInBytes = 1;
    auto data = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptr(data.data());

    const VariableSizedData varData(ptr, sizeInBytes);

    EXPECT_EQ(varData.getSize(), sizeInBytes);
    EXPECT_TRUE(varData.isValid());
    EXPECT_TRUE(nautilus::memcmp(varData.getContent(), ptr, sizeInBytes) == 0);
}

TEST_F(VariableSizedDataTest, LargeDataSize)
{
    constexpr auto sizeInBytes = 1024 * 1024; // 1MB
    auto data = createVariableSizedRandomData(sizeInBytes);
    const nautilus::val<int8_t*> ptr(data.data());

    const VariableSizedData varData(ptr, sizeInBytes);

    EXPECT_EQ(varData.getSize(), sizeInBytes);
    EXPECT_TRUE(varData.isValid());
    EXPECT_TRUE(nautilus::memcmp(varData.getContent(), ptr, sizeInBytes) == 0);
}

TEST_F(VariableSizedDataTest, CompoundConstruction)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);

    // Create compound data
    const VariableSizedData compoundData(varData1, varData2, arenaRef);

    // Verify size is sum of both
    EXPECT_EQ(compoundData.getSize(), size1 + size2);
    EXPECT_TRUE(compoundData.isValid());
    EXPECT_TRUE(nautilus::memcmp(compoundData.getContent(), data1.data(), size1) == 0);
    EXPECT_TRUE(nautilus::memcmp(compoundData.getContent() + nautilus::val<int>(size1), data2.data(), size2) == 0);
}

TEST_F(VariableSizedDataTest, CompoundContentMatchesConcatenation)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    // Create expected concatenated data
    std::vector<int8_t> expectedData;
    expectedData.insert(expectedData.end(), data1.begin(), data1.end());
    expectedData.insert(expectedData.end(), data2.begin(), data2.end());

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);

    const VariableSizedData compoundData(varData1, varData2, arenaRef);

    // Verify content matches concatenation
    EXPECT_TRUE(nautilus::memcmp(compoundData.getContent(), expectedData.data(), size1 + size2) == 0);
}

TEST_F(VariableSizedDataTest, CompoundEqualityWithRegular)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    // Create concatenated data for regular VariableSizedData
    std::vector<int8_t> concatenatedData;
    concatenatedData.insert(concatenatedData.end(), data1.begin(), data1.end());
    concatenatedData.insert(concatenatedData.end(), data2.begin(), data2.end());

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());
    const nautilus::val<int8_t*> ptrConcatenated(concatenatedData.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);
    const VariableSizedData compoundData(varData1, varData2, arenaRef);
    const VariableSizedData regularData(ptrConcatenated, size1 + size2);

    // Compound and regular with same content should be equal
    EXPECT_TRUE(compoundData == regularData);
    EXPECT_TRUE(regularData == compoundData);
}

TEST_F(VariableSizedDataTest, CompoundInequalityWithDifferentData)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);
    auto data3 = createVariableSizedRandomData(size1 + size2);

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());
    const nautilus::val<int8_t*> ptr3(data3.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);
    const VariableSizedData compoundData(varData1, varData2, arenaRef);
    const VariableSizedData differentData(ptr3, size1 + size2);

    // Compound and regular with different content should not be equal
    EXPECT_FALSE(compoundData == differentData);
    EXPECT_TRUE(compoundData != differentData);
}

TEST_F(VariableSizedDataTest, CompoundEqualityWithCompound)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);
    auto data1Copy = data1;
    auto data2Copy = data2;

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());
    const nautilus::val<int8_t*> ptr1Copy(data1Copy.data());
    const nautilus::val<int8_t*> ptr2Copy(data2Copy.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);
    const VariableSizedData varData1Copy(ptr1Copy, size1);
    const VariableSizedData varData2Copy(ptr2Copy, size2);

    const VariableSizedData compoundData1(varData1, varData2, arenaRef);
    const VariableSizedData compoundData2(varData1Copy, varData2Copy, arenaRef);

    // Two compound data with same content should be equal
    EXPECT_TRUE(compoundData1 == compoundData2);
}

TEST_F(VariableSizedDataTest, CompoundGetReference)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    const nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);
    const VariableSizedData compoundData(varData1, varData2, arenaRef);

    // getReference should return same as getContent
    EXPECT_TRUE(nautilus::memcmp(compoundData.getReference(), compoundData.getContent(), size1 + size2) == 0);
}

TEST_F(VariableSizedDataTest, CompoundCopyConstruction)
{
    auto bufferManager = BufferManager::create();
    Arena arena(bufferManager);
    nautilus::val<Arena*> arenaPtr(&arena);
    ArenaRef arenaRef(arenaPtr);

    constexpr auto size1 = 256;
    constexpr auto size2 = 128;
    auto data1 = createVariableSizedRandomData(size1);
    auto data2 = createVariableSizedRandomData(size2);

    const nautilus::val<int8_t*> ptr1(data1.data());
    const nautilus::val<int8_t*> ptr2(data2.data());

    const VariableSizedData varData1(ptr1, size1);
    const VariableSizedData varData2(ptr2, size2);
    const VariableSizedData compoundData(varData1, varData2, arenaRef);

    // Copy the compound data
    const VariableSizedData copiedCompound = compoundData;

    EXPECT_EQ(copiedCompound.getSize(), size1 + size2);
    EXPECT_TRUE(copiedCompound == compoundData);
}

}
