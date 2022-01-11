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

#include <Common/ExecutableType/Array.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include "../../util/NesBaseTest.hpp"
#include <type_traits>

namespace NES {

class ArrayTypeTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ArrayTypeTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ArrayType test class.");
    }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down ArrayType Test."); }

  protected:
};
/// Check that the null-terminator has to be present for fixed-size strings only.
TEST_F(ArrayTypeTest, testCheckNullTerminator) {
    // From supertype's constructor
    EXPECT_NO_THROW((ExecutableTypes::Array{std::array{'a', 'b', '\0'}}));
    EXPECT_THROW((ExecutableTypes::Array{std::array{'a', 'b'}}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array{std::array{1, 2, 3}}));
    EXPECT_THROW((ExecutableTypes::Array{std::array{'1', '2', '3'}}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array{std::array{'1', '2', '\0'}}));

    // From content
    EXPECT_THROW(((volatile void) ExecutableTypes::Array{'a', 'b', 'c'}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array{'a', 'b', '\0'}));
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 5>{'a', 'b', '\0'}));
    EXPECT_NO_THROW((ExecutableTypes::Array{1, 2, 3}));

    // char: From c-style array
    char const cStyleArray[3] = {'a', 'b', 'c'};
    EXPECT_THROW(((volatile void) ExecutableTypes::Array{cStyleArray}), std::runtime_error);
    char const cStyleArrayNull[4] = {'a', 'b', '\0', '0'};
    EXPECT_NO_THROW(((volatile void) ExecutableTypes::Array{cStyleArrayNull}));
    EXPECT_NO_THROW(((volatile void) ExecutableTypes::Array<char, 3>{cStyleArrayNull}));
    EXPECT_THROW(((volatile void) ExecutableTypes::Array<char, 2>{cStyleArrayNull}), std::runtime_error);
    int const cStyleArrayInt[3] = {1, 2, 3};
    EXPECT_NO_THROW(((volatile void) ExecutableTypes::Array{cStyleArrayInt}));

    EXPECT_THROW(((volatile void) ExecutableTypes::Array<char, 3>{{'a', 'b', 'c'}}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 3>{{'a', 'b', '\0'}}));
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 3>{{'1', '\0'}}));
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 3>{{'1'}}));//< automatically append \0

    // From vector
    EXPECT_THROW((ExecutableTypes::Array<char, 3>{std::vector{'a', 'b', 'c'}}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 3>{std::vector{'a', 'b', '\0'}}));
    EXPECT_THROW((ExecutableTypes::Array<char, 2>{std::vector{'a', 'b', '\0'}}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 2>{std::vector{'\0'}}));
    EXPECT_NO_THROW((ExecutableTypes::Array<int, 3>{std::vector{1, 2, 3}}));
    EXPECT_THROW((ExecutableTypes::Array<int, 2>{std::vector{1, 2, 3, 4, 5}}), std::runtime_error);
    EXPECT_THROW((ExecutableTypes::Array<int, 2>{std::vector{1}}), std::runtime_error);

    // From cstr / std::string
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 4>{"abc"}));
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 10>{"abc"}));
    EXPECT_THROW((ExecutableTypes::Array<char, 1>{"abc"}), std::runtime_error);
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 4>{std::string("abc")}));
    EXPECT_NO_THROW((ExecutableTypes::Array<char, 10>{std::string("abc")}));
    EXPECT_THROW((ExecutableTypes::Array<char, 1>{std::string("abc")}), std::runtime_error);
}

/// Test that the noexcept specifier is set correctly for initializers, i.e., noexcept iff
///   1) type is not char && (initialization by `size` elements of type or || array || c-style array)
///   2) type is char and initialization by less than `size` elements of type char
TEST_F(ArrayTypeTest, testExceptionSpecifier) {

    // From supertype's constructor
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array{std::array{'a', 'b', '\0'}}));
    EXPECT_TRUE(noexcept(ExecutableTypes::Array{std::array{1, 2, 3}}));

    // From content
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array{'a', 'b', '\0'}));
    EXPECT_TRUE(noexcept(ExecutableTypes::Array<char, 5>{'a', 'b', 'c'}));
    EXPECT_TRUE(noexcept(ExecutableTypes::Array{1, 2, 3}));

    // char: From c-style array
    char const cStyleArray[3] = {'a', 'b', 'c'};
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array{cStyleArray}));
    int const cStyleArrayInt[3] = {1, 2, 3};
    EXPECT_TRUE(noexcept(ExecutableTypes::Array{cStyleArrayInt}));

    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 3>{{'a', 'b', '\0'}}));
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 3>{{'1', '\0'}}));
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 3>{{'1'}}));
    EXPECT_TRUE(noexcept(ExecutableTypes::Array<int, 3>{{1}}));
    EXPECT_TRUE(noexcept(ExecutableTypes::Array<int, 3>{{1, 2, 3}}));

    // From vector
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 4>{std::vector{'a', 'b', 'c'}}));
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 4>{std::vector{'a', 'b', 'c', '\0'}}));
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<int, 3>{std::vector{1, 2, 3}}));

    // From cstr / std::string
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 4>{"abc"}));
    EXPECT_TRUE(!noexcept(ExecutableTypes::Array<char, 4>{std::string("abc")}));
}

/// @brief Utility for checking equality
template<std::size_t s, typename J>
auto eq(ExecutableTypes::Array<char, s> const& l, J r) -> bool {
    return std::string(&(l[0])) == r;
}
template<typename T, std::size_t s>
auto eq(ExecutableTypes::Array<T, s> const& l, std::vector<T>&& r) -> bool {
    if (s != r.size()) {
        return false;
    }

    for (std::size_t i{0}; i < s; ++i) {
        if (l[i] != r[i]) {
            return false;
        }
    }
    return true;
}

/// @brief Test type, size and content of ExecutableTypes::Arrays.
TEST_F(ArrayTypeTest, testInitialization) {

    // From supertype's constructor
    {
        ExecutableTypes::Array ca{std::array{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(ca)::type, char>) );
        EXPECT_TRUE(ca.size == 3);
        EXPECT_TRUE(eq(ca, "ab"));
        EXPECT_TRUE(!eq(ca, "a"));
    }
    {
        ExecutableTypes::Array cai = {std::array{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cai)::type, char>) );
        EXPECT_TRUE(cai.size == 3);
        EXPECT_TRUE(eq(cai, "ab"));
        EXPECT_TRUE(!eq(cai, "a"));
    }
    {

        ExecutableTypes::Array ia{std::array{1, 2, 3}};
        EXPECT_TRUE((std::is_same_v<typename decltype(ia)::type, int>) );
        EXPECT_TRUE(ia.size == 3);
        EXPECT_TRUE(eq(ia, {1, 2, 3}));
        EXPECT_TRUE(!eq(ia, {1, 2}));
    }
    {
        ExecutableTypes::Array iai = {std::array{1, 2, 3}};
        EXPECT_TRUE((std::is_same_v<typename decltype(iai)::type, int>) );
        EXPECT_TRUE(iai.size == 3);
        EXPECT_TRUE(eq(iai, {1, 2, 3}));
        EXPECT_TRUE(!eq(iai, {1, 2}));
    }

    // From content
    {
        ExecutableTypes::Array cc{'a', 'b', '\0'};
        EXPECT_TRUE((std::is_same_v<typename decltype(cc)::type, char>) );
        EXPECT_TRUE(cc.size == 3);
        EXPECT_TRUE(eq(cc, "ab"));
        EXPECT_TRUE(!eq(cc, "ac"));
    }
    {
        ExecutableTypes::Array cci = {'a', 'b', '\0'};
        EXPECT_TRUE((std::is_same_v<typename decltype(cci)::type, char>) );
        EXPECT_TRUE(cci.size == 3);
        EXPECT_TRUE(eq(cci, "ab"));
        EXPECT_TRUE(!eq(cci, "ac"));
    }
    {

        ExecutableTypes::Array<char, 5> ccs{'a', 'b', '\0'};
        EXPECT_TRUE((std::is_same_v<typename decltype(ccs)::type, char>) );
        EXPECT_TRUE(ccs.size == 5);
        EXPECT_TRUE(eq(ccs, "ab"));
        EXPECT_TRUE(!eq(ccs, "ac"));
    }
    {
        ExecutableTypes::Array<char, 5> ccsi{'a', 'b', '\0'};
        EXPECT_TRUE((std::is_same_v<typename decltype(ccsi)::type, char>) );
        EXPECT_TRUE(ccsi.size == 5);
        EXPECT_TRUE(eq(ccsi, "ab"));
        EXPECT_TRUE(!eq(ccsi, "ac"));
    }
    {

        ExecutableTypes::Array ic{1, 2, 3};
        EXPECT_TRUE((std::is_same_v<typename decltype(ic)::type, int>) );
        EXPECT_TRUE(ic.size == 3);
        EXPECT_TRUE(eq(ic, {1, 2, 3}));
        EXPECT_TRUE(!eq(ic, {1, 2, 5}));
        EXPECT_TRUE(!eq(ic, {1, 2}));
    }
    {
        ExecutableTypes::Array ici{1, 2, 3};
        EXPECT_TRUE((std::is_same_v<typename decltype(ici)::type, int>) );
        EXPECT_TRUE(ici.size == 3);
        EXPECT_TRUE(eq(ici, {1, 2, 3}));
        EXPECT_TRUE(!eq(ici, {1, 2, 5}));
        EXPECT_TRUE(!eq(ici, {1, 2}));
    }

    // char: From c-style array
    {
        char const cStyleArrayNull[4] = {'a', 'b', '\0', '0'};
        ExecutableTypes::Array{cStyleArrayNull};
        ExecutableTypes::Array<char, 3> cca{cStyleArrayNull};
        EXPECT_TRUE((std::is_same_v<typename decltype(cca)::type, char>) );
        EXPECT_TRUE(cca.size == 3);
        EXPECT_TRUE(eq(cca, "ab"));
    }
    {
        char const cStyleArrayNull[4] = {'a', 'b', '\0', '0'};
        ExecutableTypes::Array<char, 3> ccai{cStyleArrayNull};
        EXPECT_TRUE((std::is_same_v<typename decltype(ccai)::type, char>) );
        EXPECT_TRUE(ccai.size == 3);
        EXPECT_TRUE(eq(ccai, "ab"));
    }
    {

        int const cStyleArrayInt[3] = {1, 2, 3};
        ExecutableTypes::Array ica{cStyleArrayInt};
        EXPECT_TRUE((std::is_same_v<typename decltype(ica)::type, int>) );
        EXPECT_TRUE(ica.size == 3);
        EXPECT_TRUE(eq(ica, {1, 2, 3}));
    }
    {
        int const cStyleArrayInt[3] = {1, 2, 3};
        ExecutableTypes::Array icai = {cStyleArrayInt};
        EXPECT_TRUE((std::is_same_v<typename decltype(icai)::type, int>) );
        EXPECT_TRUE(icai.size == 3);
        EXPECT_TRUE(eq(icai, {1, 2, 3}));
    }
    {
        ExecutableTypes::Array<char, 3> cdbi{{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cdbi)::type, char>) );
        EXPECT_TRUE(cdbi.size == 3);
        EXPECT_TRUE(eq(cdbi, "ab"));
    }
    {
        ExecutableTypes::Array<char, 3> cdbii = {{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cdbii)::type, char>) );
        EXPECT_TRUE(cdbii.size == 3);
        EXPECT_TRUE(eq(cdbii, "ab"));
    }
    {

        ExecutableTypes::Array<char, 3> cdbil{{'1'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cdbil)::type, char>) );
        EXPECT_TRUE(cdbil.size == 3);
        EXPECT_TRUE(eq(cdbil, "1"));
    }
    {
        ExecutableTypes::Array<char, 3> cdbili = {{'1'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cdbili)::type, char>) );
        EXPECT_TRUE(cdbili.size == 3);
        EXPECT_TRUE(eq(cdbili, "1"));
    }
    {}

    // From vector
    {
        ExecutableTypes::Array<char, 3> cv{std::vector{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cv)::type, char>) );
        EXPECT_TRUE(cv.size == 3);
        EXPECT_TRUE(eq(cv, "ab"));
    }
    {
        ExecutableTypes::Array<char, 3> cvi = {std::vector{'a', 'b', '\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cvi)::type, char>) );
        EXPECT_TRUE(cvi.size == 3);
        EXPECT_TRUE(eq(cvi, "ab"));
    }
    {

        ExecutableTypes::Array<char, 2> cv0{std::vector{'\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cv0)::type, char>) );
        EXPECT_TRUE(cv0.size == 2);
        EXPECT_TRUE(eq(cv0, std::string()));
    }
    {
        ExecutableTypes::Array<char, 2> cv0i = {std::vector{'\0'}};
        EXPECT_TRUE((std::is_same_v<typename decltype(cv0i)::type, char>) );
        EXPECT_TRUE(cv0i.size == 2);
        EXPECT_TRUE(eq(cv0i, std::string()));
    }
    {

        ExecutableTypes::Array<int, 3> iv{std::vector{1, 2, 3}};
        EXPECT_TRUE((std::is_same_v<typename decltype(iv)::type, int>) );
        EXPECT_TRUE(iv.size == 3);
        EXPECT_TRUE(eq(iv, {1, 2, 3}));
        EXPECT_TRUE(!eq(iv, {0, 2, 3}));
    }
    {
        ExecutableTypes::Array<int, 3> ivi = {std::vector{1, 2, 3}};
        EXPECT_TRUE((std::is_same_v<typename decltype(ivi)::type, int>) );
        EXPECT_TRUE(ivi.size == 3);
        EXPECT_TRUE(eq(ivi, {1, 2, 3}));
        EXPECT_TRUE(!eq(ivi, {0, 2, 3}));
    }

    // From cstr / std::string
    {
        ExecutableTypes::Array<char, 4> cstrF{"abc"};
        EXPECT_TRUE((std::is_same_v<typename decltype(cstrF)::type, char>) );
        EXPECT_TRUE(cstrF.size == 4);
        EXPECT_TRUE(eq(cstrF, "abc"));
        EXPECT_TRUE(!eq(cstrF, "a c"));
    }
    {
        ExecutableTypes::Array<char, 4> cstrF = {"abc"};
        EXPECT_TRUE((std::is_same_v<typename decltype(cstrF)::type, char>) );
        EXPECT_TRUE(cstrF.size == 4);
        EXPECT_TRUE(eq(cstrF, "abc"));
        EXPECT_TRUE(!eq(cstrF, "a c"));
    }
    {

        ExecutableTypes::Array<char, 10> cstrP{"abc"};
        EXPECT_TRUE((std::is_same_v<typename decltype(cstrP)::type, char>) );
        EXPECT_TRUE(cstrP.size == 10);
        EXPECT_TRUE(eq(cstrP, "abc"));
        EXPECT_TRUE(!eq(cstrP, "dbc"));
    }
    {
        ExecutableTypes::Array<char, 10> cstrP = {"abc"};
        EXPECT_TRUE((std::is_same_v<typename decltype(cstrP)::type, char>) );
        EXPECT_TRUE(cstrP.size == 10);
        EXPECT_TRUE(eq(cstrP, "abc"));
        EXPECT_TRUE(!eq(cstrP, "dabc"));
    }
    {

        ExecutableTypes::Array<char, 4> strF{std::string("abc")};
        EXPECT_TRUE((std::is_same_v<typename decltype(strF)::type, char>) );
        EXPECT_TRUE(strF.size == 4);
        EXPECT_TRUE(eq(strF, "abc"));
        EXPECT_TRUE(!eq(strF, "aoc"));
    }
    {
        ExecutableTypes::Array<char, 4> strF = {std::string("abc")};
        EXPECT_TRUE((std::is_same_v<typename decltype(strF)::type, char>) );
        EXPECT_TRUE(strF.size == 4);
        EXPECT_TRUE(eq(strF, "abc"));
        EXPECT_TRUE(!eq(strF, "ac"));
    }
    {

        ExecutableTypes::Array<char, 10> strP{std::string("abc")};
        EXPECT_TRUE((std::is_same_v<typename decltype(strP)::type, char>) );
        EXPECT_TRUE(strP.size == 10);
        EXPECT_TRUE(eq(strP, "abc"));
    }
    {
        ExecutableTypes::Array<char, 10> strP = {std::string("abc")};
        EXPECT_TRUE((std::is_same_v<typename decltype(strP)::type, char>) );
        EXPECT_TRUE(strP.size == 10);
        EXPECT_TRUE(eq(strP, "abc"));
    }
}

}// namespace NES
