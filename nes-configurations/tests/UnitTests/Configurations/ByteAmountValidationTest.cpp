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

#include <Configurations/Validation/ByteAmountValidation.hpp>
#include <gtest/gtest.h>

namespace NES
{

class ByteAmountValidationTest : public testing::Test
{
};

TEST_F(ByteAmountValidationTest, testValidByteAmounts)
{
    ByteAmountValidation validation;

    EXPECT_TRUE(validation.isValid("1024"));
    EXPECT_TRUE(validation.isValid("1024B"));
    EXPECT_TRUE(validation.isValid("1024b"));
    EXPECT_TRUE(validation.isValid("1KiB"));
    EXPECT_TRUE(validation.isValid("1Ki"));
    EXPECT_TRUE(validation.isValid("1.5Gi"));
    EXPECT_TRUE(validation.isValid("2.5K"));
    EXPECT_TRUE(validation.isValid("0"));
    EXPECT_TRUE(validation.isValid("100M"));
    EXPECT_TRUE(validation.isValid("100Mi"));
    EXPECT_TRUE(validation.isValid("100G"));
    EXPECT_TRUE(validation.isValid("100Gi"));
    EXPECT_TRUE(validation.isValid("100T"));
    EXPECT_TRUE(validation.isValid("100Ti"));
    EXPECT_TRUE(validation.isValid("100P"));
    EXPECT_TRUE(validation.isValid("100Pi"));
    EXPECT_TRUE(validation.isValid("100E"));
    EXPECT_TRUE(validation.isValid("10Ei"));

    EXPECT_EQ(parseByteAmount("1024").value, 1024ULL);
    EXPECT_EQ(parseByteAmount("1KiB").value, 1024ULL);
    EXPECT_EQ(parseByteAmount("1Ki").value, 1024ULL);
    EXPECT_EQ(parseByteAmount("1.5Gi").value, static_cast<uint64_t>(1.5 * 1024 * 1024 * 1024));
    EXPECT_EQ(parseByteAmount("2.5K").value, 2500ULL);
}

TEST_F(ByteAmountValidationTest, testInvalidByteAmounts)
{
    ByteAmountValidation validation;

    EXPECT_FALSE(validation.isValid("-1024"));
    EXPECT_FALSE(validation.isValid("1024X"));
    EXPECT_FALSE(validation.isValid("KiB"));
    EXPECT_FALSE(validation.isValid("1.5.5Gi"));
    EXPECT_FALSE(validation.isValid(""));
    EXPECT_FALSE(validation.isValid("   "));

    EXPECT_ANY_THROW(parseByteAmount("-1024"));
    EXPECT_ANY_THROW(parseByteAmount("1024X"));
    EXPECT_ANY_THROW(parseByteAmount("KiB"));
}

TEST_F(ByteAmountValidationTest, testOverflow)
{
    ByteAmountValidation validation;

    // 18446744073709551615 is UINT64_MAX, which is fine
    EXPECT_TRUE(validation.isValid("18446744073709551615"));
    EXPECT_EQ(parseByteAmount("18446744073709551615").value, UINT64_MAX);

    // One more should overflow
    EXPECT_FALSE(validation.isValid("18446744073709551616"));
    EXPECT_ANY_THROW(parseByteAmount("18446744073709551616"));

    // Huge double value overflow
    EXPECT_FALSE(validation.isValid("1000Ei"));
    EXPECT_ANY_THROW(parseByteAmount("1000Ei"));
}

}
