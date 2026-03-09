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
#include <exception>
#include <ios>
#include <sstream>
#include <string>
#include <utility>
#include <rapidcheck.h>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <gtest/gtest.h>
#include <nautilus/function.hpp>
#include <nautilus/std/sstream.h>
#include <rapidcheck/gtest.h>
#include <std/cstring.h>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

/// Creates a nautilus val pointer from a string's data. The caller must keep the string data alive.
nautilus::val<int8_t*> toValPtr(const std::string& str)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-pro-type-reinterpret-cast) nautilus val requires mutable int8_t* but string exposes const char*
    return {const_cast<int8_t*>(reinterpret_cast<const int8_t*>(str.data()))};
}

/// Creates a VariableSizedData from a string. The caller must keep the string data alive.
VariableSizedData createFromString(const std::string& str)
{
    return VariableSizedData(toValPtr(str), str.size());
}

} /// anonymous namespace

RC_GTEST_PROP(VariableSizedDataTest, construction, (const std::string& data))
{
    const auto vsd = createFromString(data);
    const VarVal varVal{vsd};
    RC_ASSERT(varVal.cast<VariableSizedData>().getSize() == data.size());
    if (!data.empty())
    {
        const auto dataPtr = toValPtr(data);
        RC_ASSERT(nautilus::memcmp(varVal.cast<VariableSizedData>().getContent(), dataPtr, data.size()) == 0);
    }
}

RC_GTEST_PROP(VariableSizedDataTest, copyConstruction, (const std::string& data))
{
    const auto vsd = createFromString(data);
    const VarVal original{vsd};

    /// Copy via operator=
    const VarVal copied = original; /// NOLINT(performance-unnecessary-copy-initialization) intentional copy to test copy semantics
    RC_ASSERT(copied.cast<VariableSizedData>().getSize() == data.size());

    /// Copy via copy constructor
    const VarVal copied2(original); /// NOLINT(performance-unnecessary-copy-initialization) intentional copy to test copy constructor
    RC_ASSERT(copied2.cast<VariableSizedData>().getSize() == data.size());

    if (!data.empty())
    {
        const auto dataPtr = toValPtr(data);
        RC_ASSERT(nautilus::memcmp(copied.cast<VariableSizedData>().getContent(), dataPtr, data.size()) == 0);
        RC_ASSERT(nautilus::memcmp(copied2.cast<VariableSizedData>().getContent(), dataPtr, data.size()) == 0);
    }
}

RC_GTEST_PROP(VariableSizedDataTest, moveConstruction, (const std::string& data))
{
    /// Move via operator=
    {
        const auto vsd = createFromString(data);
        VarVal original{vsd};
        const VarVal moved = std::move(original);
        RC_ASSERT(moved.cast<VariableSizedData>().getSize() == data.size());
        if (!data.empty())
        {
            const auto dataPtr = toValPtr(data);
            RC_ASSERT(nautilus::memcmp(moved.cast<VariableSizedData>().getContent(), dataPtr, data.size()) == 0);
        }
    }

    /// Move via move constructor
    {
        const auto vsd = createFromString(data);
        VarVal original{vsd};
        const VarVal moved(std::move(original));
        RC_ASSERT(moved.cast<VariableSizedData>().getSize() == data.size());
        if (!data.empty())
        {
            const auto dataPtr = toValPtr(data);
            RC_ASSERT(nautilus::memcmp(moved.cast<VariableSizedData>().getContent(), dataPtr, data.size()) == 0);
        }
    }
}

RC_GTEST_PROP(VariableSizedDataTest, equalityMatchesString, (const std::string& lhs, const std::string& rhs))
{
    const auto vsdLhs = createFromString(lhs);
    const auto vsdRhs = createFromString(rhs);
    RC_ASSERT((lhs == rhs) == static_cast<bool>(vsdLhs == vsdRhs));
    RC_ASSERT((lhs != rhs) == static_cast<bool>(vsdLhs != vsdRhs));
}

RC_GTEST_PROP(VariableSizedDataTest, lessThanMatchesString, (const std::string& lhs, const std::string& rhs))
{
    const auto vsdLhs = createFromString(lhs);
    const auto vsdRhs = createFromString(rhs);
    RC_ASSERT((lhs < rhs) == static_cast<bool>(vsdLhs < vsdRhs));
    RC_ASSERT((rhs < lhs) == static_cast<bool>(vsdRhs < vsdLhs));
}

void compareStringProxy(const char* actual, const char* expected)
{
    const std::string actualStr(actual);
    const std::string expectedStr(expected);
    EXPECT_EQ(expectedStr, actualStr);
}

RC_GTEST_PROP(VariableSizedDataTest, ostreamOutput, (const std::string& data))
{
    RC_PRE(!data.empty());
    const auto vsd = createFromString(data);
    const VarVal varVal{vsd};

    /// Build expected output
    std::stringstream expectedOutput;
    expectedOutput << "Size(" << data.size() << "): ";
    for (const auto byte : data)
    {
        expectedOutput << std::hex << static_cast<int>(static_cast<uint8_t>(byte)) << " ";
    }
    nautilus::stringstream expected;
    expected << expectedOutput.str().c_str();

    nautilus::stringstream output;
    output << varVal;

    nautilus::invoke(compareStringProxy, output.str().c_str(), expected.str().c_str());
}

}
