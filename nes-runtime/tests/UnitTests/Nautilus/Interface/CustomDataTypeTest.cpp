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

#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class CustomDataTypeTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CustomDataTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CustomDataTypeTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down CustomDataTypeTest test class."); }
};

class CustomType : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<CustomType>();
    CustomType(Value<> x, Value<> y) : Any(&type), x(x), y(y){};

    auto add(const CustomType& other) const { return create<CustomType>(x + other.x, y + other.y); }

    auto power(const CustomType& other) const { return create<Int64>(x * other.x - y); }

    Value<> x;
    Value<> y;
    AnyPtr copy() override { return create<CustomType>(x, y); }
};

class CustomTypeInvocationPlugin : public InvocationPlugin {
  public:
    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<CustomType>(left.value) && isa<CustomType>(right.value)) {
            auto& ct1 = left.getValue().staticCast<CustomType>();
            auto& ct2 = right.getValue().staticCast<CustomType>();
            return Value(ct1.add(ct2));
        } else {
            return std::nullopt;
        }
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<CustomTypeInvocationPlugin> cPlugin;

TEST_F(CustomDataTypeTest, customCustomDataTypeTest) {
    Value<Int64> x = (int64_t) 32;
    Value<Int64> y = (int64_t) 32;

    auto c1 = Value<CustomType>(CustomType(x, y));
    auto c2 = Value<CustomType>(CustomType(x, y));

    auto c3 = c1 + c2;
    c1 = c2;
    NES_DEBUG(c3.value);
}

TEST_F(CustomDataTypeTest, customTimeStampTypeTest) {
    long ms = 1666798551744;// Wed Oct 26 2022 15:35:51
    std::chrono::hours dur(ms);
    auto c1 = Value<TimeStamp>(TimeStamp((int64_t) dur.count()));
    ASSERT_EQ(cast<IR::Types::IntegerStamp>(c1.value->getType())->getNumberOfBits(), 64);
    ASSERT_EQ(c1.getValue().toString(), std::to_string(ms));

    const TimeStamp c2 = TimeStamp((int64_t) dur.count());
    auto c3 = c1 + c2;
    ASSERT_EQ(c3.getValue().toString(), std::to_string(3333597103488));
    const TimeStamp c4 = TimeStamp((int64_t) dur.count() - 1000);
    /* tests the functions greater and less than, equals */
    ASSERT_EQ((c1 > c4), true);
    ASSERT_EQ((c4 < c1), true);
    ASSERT_EQ((c1 == c2), true);
    ASSERT_EQ((c1 == c3), false);
    auto seconds = c1->getSeconds();
    ASSERT_EQ(seconds->toString(), std::to_string(51));
    auto minutes = c1->getMinutes();
    ASSERT_EQ(minutes->toString(), std::to_string(35));
    auto hours = c1->getHours();
    ASSERT_EQ(hours->toString(), std::to_string(15));
    auto days = c1->getDays();
    ASSERT_EQ(days->toString(), std::to_string(26));
    auto months = c1->getMonths();
    ASSERT_EQ(months->toString(), std::to_string(10));
    auto years = c1->getYears();
    ASSERT_EQ(years->toString(), std::to_string(2022));
}

}// namespace NES::Nautilus