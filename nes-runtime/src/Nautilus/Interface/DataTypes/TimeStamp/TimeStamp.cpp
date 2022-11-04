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
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <chrono>
#include <ctime>
#include <iomanip>
using namespace std::chrono_literals;
namespace NES::Nautilus {

TimeStamp::TimeStamp(Value<> x) : Any(&type), milliseconds(x) {
    if (x->isType<Int64>()) {
        milliseconds = x;
    } else {
        NES_THROW_RUNTIME_ERROR("Can not make a TimeStamp object out of" << x);
        //TODO convert a string such as 2009-06-15T13:45:30 to millis (DBPRO'22 issue)
    }
};

AnyPtr TimeStamp::copy() { return create<TimeStamp>(milliseconds); }
AnyPtr TimeStamp::add(const TimeStamp& other) const { return create<TimeStamp>(milliseconds + other.milliseconds); }
std::shared_ptr<Boolean> TimeStamp::equals(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds == otherValue.milliseconds);
}
std::shared_ptr<Boolean> TimeStamp::lessThan(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds < otherValue.milliseconds);
}
std::shared_ptr<Boolean> TimeStamp::greaterThan(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds > otherValue.milliseconds);
}

/* this method converts long milliseconds to a clock time representation */
tm convertToUTC_TM(int64_t milliseconds) {
    std::chrono::duration<int64_t, std::milli> dur(milliseconds);
    auto tp = std::chrono::system_clock::time_point(std::chrono::duration_cast<std::chrono::system_clock::duration>(dur));
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    return *gmtime(&tt);
}

Nautilus::IR::Types::StampPtr TimeStamp::getType() const { return Nautilus::IR::Types::StampFactory::createUInt64Stamp(); }

int64_t getMilliSecondsChrono(int64_t milliseconds) { return milliseconds; }
Value<> TimeStamp::getMilliSeconds() {
    return FunctionCall<>("getMilliSeconds", getMilliSecondsChrono, milliseconds.as<Int64>());
}
int64_t getSecondsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_sec;
}
Value<> TimeStamp::getSeconds() { return FunctionCall<>("getSeconds", getSecondsChrono, milliseconds.as<Int64>()); }
int64_t getMinutesChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_min;
}
Value<> TimeStamp::getMinutes() { return FunctionCall<>("getMinutes", getMinutesChrono, milliseconds.as<Int64>()); }
int64_t getHoursChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_hour;
}
Value<> TimeStamp::getHours() { return FunctionCall<>("getHours", getHoursChrono, milliseconds.as<Int64>()); }
int64_t getDaysChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_mday;
}
Value<> TimeStamp::getDays() { return FunctionCall<>("getDays", getDaysChrono, milliseconds.as<Int64>()); }
int64_t getMonthsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_mon + 1;
}
Value<> TimeStamp::getMonths() { return FunctionCall<>("getMonths", getMonthsChrono, milliseconds.as<Int64>()); }
int64_t getYearsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_year + 1900;
}
Value<> TimeStamp::getYears() { return FunctionCall<>("getYears", getYearsChrono, milliseconds.as<Int64>()); }

Value<> TimeStamp::getValue() { return milliseconds; }
std::string getStringFromMillis(int64_t milliseconds) {
    using time_point = std::chrono::system_clock::time_point;
    time_point milliseconds_timepoint{std::chrono::duration_cast<time_point::duration>(std::chrono::nanoseconds(milliseconds))};
    std::time_t t = std::chrono::system_clock::to_time_t(milliseconds_timepoint);
    return std::ctime(&t);
}
std::string TimeStamp::toString() {
    return milliseconds->toString();
    //return FunctionCall<>("toString", getStringFromMillis, milliseconds.as<Int64>());
}

}// namespace NES::Nautilus