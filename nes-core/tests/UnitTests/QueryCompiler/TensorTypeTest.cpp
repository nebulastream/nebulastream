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

#include <Common/ExecutableType/Tensor.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <type_traits>

namespace NES {

class TensorTypeTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TensorTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TensorType test class.");
    }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down TensorType Test."); }

  protected:
};
/// Check that the null-terminator has to be present for fixed-size strings only.
TEST_F(TensorTypeTest, testCorrectCreationOfTensor) {
    // From supertype's constructor
    EXPECT_NO_THROW((ExecutableTypes::Tensor<int, 5, 5>()));
    EXPECT_THROW((ExecutableTypes::Tensor<int, 5, 10>()), std::out_of_range);
    EXPECT_NO_THROW((ExecutableTypes::Tensor<int, 8, 2, 4>()));
    EXPECT_THROW((ExecutableTypes::Tensor<int, 9, 2, 4>()), std::out_of_range);
    EXPECT_NO_THROW((ExecutableTypes::Tensor<int, 30, 2, 3, 5>()));
    EXPECT_THROW((ExecutableTypes::Tensor<int, 31, 2, 3, 5>()), std::out_of_range);
    EXPECT_NO_THROW((ExecutableTypes::Tensor<int, 80, 2, 4, 5, 2>()));
}

/// Test that the noexcept specifier is set correctly for initializers, i.e., noexcept iff
///   1) type is not char && (initialization by `size` elements of type or || array || c-style array)
///   2) type is char and initialization by less than `size` elements of type char
TEST_F(TensorTypeTest, testValueInitializationAndIndexing) {

    auto tens1 = ExecutableTypes::Tensor<int, 3, 3>{std::array{1, 2, 3}};
    EXPECT_EQ(2, tens1(1));

    //seperate definition and init of 3x3 matrix
    auto tens2 = ExecutableTypes::Tensor<int, 9, 3, 3>();
    tens2 = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    //check if indexing works correctly
    EXPECT_EQ(1, tens2(0, 0));
    EXPECT_EQ(4, tens2(1, 0));
    EXPECT_EQ(8, tens2(2, 1));
    EXPECT_EQ(9, tens2(2, 2));
    EXPECT_FALSE(noexcept(tens2(0, 10)));
    EXPECT_FALSE(noexcept(tens2(4, 0)));
    EXPECT_FALSE(noexcept(tens2(4)));
    EXPECT_THROW(tens2(2), std::runtime_error);

    //init from c style array
    int const cStyleArrayInt[3] = {1, 2, 3};
    auto tens3 = ExecutableTypes::Tensor<int, 3, 3>{cStyleArrayInt};
    EXPECT_EQ(2, tens3(1));
    int const cStyleMatrixInt[9] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto tens4 = ExecutableTypes::Tensor<int, 9, 3, 3>{cStyleMatrixInt};
    EXPECT_EQ(9, tens4(2, 2));

    //init from implicit array
    auto tens5 = ExecutableTypes::Tensor<int, 12, 2, 3, 2>{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}};
    EXPECT_EQ(8, tens5(1,0,1));

    // From vector
    auto tens6 = ExecutableTypes::Tensor<int, 3, 3>{std::vector{1, 2, 3}};
    EXPECT_EQ(2, tens6(1));

};

}// namespace NES
