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

#include <string>
#include <Configurations/Validation/PowerOfTwoValidation.hpp>
#include <gtest/gtest.h>

GTEST_TEST(PowerOfTwoValidationTest, AcceptsPowersOfTwo)
{
    const NES::PowerOfTwoValidation validation;
    auto assertValid = [&](const std::string& value) { EXPECT_TRUE(validation.isValid(value)) << value; };

    assertValid("1");
    assertValid("2");
    assertValid("4");
    assertValid("8");
    assertValid("64");
    assertValid("4096");
    assertValid("1048576");
    assertValid("9223372036854775808"); /// 2^63
}

GTEST_TEST(PowerOfTwoValidationTest, RejectsNonPowersOfTwoAndGarbage)
{
    const NES::PowerOfTwoValidation validation;
    auto assertInvalid = [&](const std::string& value) { EXPECT_FALSE(validation.isValid(value)) << value; };

    assertInvalid("0"); /// zero is not a positive power of two
    assertInvalid("3");
    assertInvalid("6");
    assertInvalid("1000");
    assertInvalid(""); /// empty
    assertInvalid("abc"); /// non-numeric
    assertInvalid("-4"); /// negative / leading sign
    assertInvalid("4096 "); /// trailing characters -> partial parse
    assertInvalid("0x10"); /// hex literal not accepted
    assertInvalid("18446744073709551616"); /// 2^64 -> overflows uint64
}
