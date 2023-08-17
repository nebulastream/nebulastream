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
#include <Util/StdInt.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class CustomDataTypeTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TextTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TextTypeTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1024);
        NES_DEBUG("Setup TextTypeTest test case.")
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down TextTypeTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
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
    Value<Int64> x(32_s64);
    Value<Int64> y(32_s64);

    auto c1 = Value<CustomType>(CustomType(x, y));
    auto c2 = Value<CustomType>(CustomType(x, y));

    auto c3 = c1 + c2;
    c1 = c2;
    std::stringstream value;
    value << c3.value;
    NES_DEBUG("{}", value.str());
}

TEST_F(CustomDataTypeTest, customTimeStampTypeBaseTest) {
    long ms = 1666798551744;// Wed Oct 26 2022 15:35:51
    std::chrono::hours dur(ms);
    auto c1 = Value<TimeStamp>(TimeStamp((uint64_t) dur.count()));
    EXPECT_EQ(cast<IR::Types::IntegerStamp>(c1.value->getType())->getNumberOfBits(), 64);
    EXPECT_EQ(c1->getMilliSeconds(), (uint64_t) ms);

    const TimeStamp c2 = TimeStamp((uint64_t) dur.count());
    auto c3 = c1 + c2;
    EXPECT_EQ(c3.as<TimeStamp>()->getMilliSeconds(), 3333597103488_u64);
    const TimeStamp c4 = TimeStamp((uint64_t) dur.count() - 1000);
    /** tests the functions greater and less than, equals */
    EXPECT_EQ((c1 > c4), true);
    EXPECT_EQ((c4 < c1), true);
    EXPECT_EQ((c1 == c2), true);
    EXPECT_EQ((c1 == c3), false);
    /** tests the functions get a day part, seconds to year */
    auto seconds = c1->getSeconds();
    EXPECT_EQ(seconds->toString(), std::to_string(51));
    auto minutes = c1->getMinutes();
    EXPECT_EQ(minutes->toString(), std::to_string(35));
    auto hours = c1->getHours();
    EXPECT_EQ(hours->toString(), std::to_string(15));
    auto days = c1->getDays();
    EXPECT_EQ(days->toString(), std::to_string(26));
    auto months = c1->getMonths();
    EXPECT_EQ(months->toString(), std::to_string(10));
    auto years = c1->getYears();
    EXPECT_EQ(years->toString(), std::to_string(2022));
}

TEST_F(CustomDataTypeTest, customTimeStampTypeExtendions) {
    /** Test für century functions*/
    long ms = 1666798551744;// Wed Oct 26 2022 15:35:51
    std::chrono::milliseconds dur(ms);
    auto c1 = Value<TimeStamp>(TimeStamp((uint64_t) dur.count()));
    auto centuryc1 = c1->century();
    EXPECT_EQ(centuryc1, 21_u64);
    long ms2 = 570207551;// Wed Jan 07 1970 14:23:27
    std::chrono::milliseconds dur1(ms2);
    auto c2 = Value<TimeStamp>(TimeStamp((uint64_t) dur1.count()));
    auto centuryc2 = c2->century();
    EXPECT_EQ(centuryc2, 20_u64);
    /** Test für age functions*/
    auto age = c2->difference(c1);
    EXPECT_EQ(age, 1666228344193_u64);
    auto age2 = c1->difference(c2);
    EXPECT_EQ(age2, 1666228344193_u64);
    /** Test für weekdayName */
    auto weekdayName = c1->getWeekdayName();
    EXPECT_EQ(weekdayName, Value<Text>("Wednesday"));
    long ms3 = 1666000551744;//Mon Oct 17 2022 09:55:51
    std::chrono::milliseconds dur2(ms3);
    auto c3 = Value<TimeStamp>(TimeStamp((uint64_t) dur2.count()));
    auto weekdayNameMon = c3->getWeekdayName();
    EXPECT_EQ(weekdayNameMon, Value<Text>("Monday"));
}

TEST_F(CustomDataTypeTest, customTimeStampTypeConstructurTest) {
    //Test String Constructor
    auto textValue = Value<Text>("1970-01-07T14:23:27");
    auto c3 = Value<TimeStamp>(TimeStamp((Value<Text>) textValue));
    EXPECT_EQ(c3->getMilliSeconds(), 570207000_u64);
    //
    auto textValue1 = Value<Text>("1970-01-07");
    auto c4 = Value<TimeStamp>(TimeStamp((Value<Text>) textValue1));
    EXPECT_EQ(c4->getMilliSeconds(), 518400000_u64);
}

TEST_F(CustomDataTypeTest, customTimeStampTypeIntervalTest) {
    std::chrono::milliseconds dur(3600000L);
    auto dayParts = Value<Text>("1970 years 1 day 1 hour ");
    auto c1 = Value<TimeStamp>(TimeStamp((uint64_t) dur.count()));
    auto interval = c1->interval();
    EXPECT_EQ(interval, dayParts);

    std::chrono::milliseconds dur1(570207551L);
    auto c2 = Value<TimeStamp>(TimeStamp((uint64_t) dur1.count()));
    auto interval1 = c2->interval();
    auto dayParts2 = Value<Text>("1970 years 7 days 14 hours 23 minutes 27 seconds");
    EXPECT_EQ(interval1, dayParts2);

    std::chrono::milliseconds duris(0L);
    auto c3 = Value<TimeStamp>(TimeStamp((uint64_t) duris.count()));
    auto dayParts3 = Value<Text>("1970 years 1 day ");
    auto interval2 = c3->interval();
    EXPECT_TRUE((Boolean) interval2->equals(dayParts3));

    auto c4 = Value<TimeStamp>(TimeStamp((uint64_t) duris.count()));
    auto textzero = Value<Text>("1970 years 1 day und ganz viel Quatsch");
    auto intervall1 = c4->interval();
    EXPECT_FALSE((Boolean) intervall1->equals(textzero));
}

}// namespace NES::Nautilus