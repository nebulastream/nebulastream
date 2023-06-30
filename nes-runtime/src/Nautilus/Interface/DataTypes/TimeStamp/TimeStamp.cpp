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
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <ctime>
#include <iomanip>
using namespace std::chrono_literals;
namespace NES::Nautilus {

/* this method converts long milliseconds to a clock time representation */
tm convertToUTC_TM(int64_t milliseconds) {
    std::chrono::duration<int64_t, std::milli> dur(milliseconds);
    auto tp = std::chrono::system_clock::time_point(std::chrono::duration_cast<std::chrono::system_clock::duration>(dur));
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    return *gmtime(&tt);
}

// Convert a string in the format "YYYY-MM-DDTHH:MM:SS" to milliseconds since the year 0 (UTC (Universal Time Coordinated)). //1970-01-07T14:23:27
uint64_t stringtomillisecondsproxy(TextValue* t) {
    // Parse the input string to extract the date and time.
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    std::string timeString = t->c_str();
    if (timeString.find('T') != std::string::npos) {
        std::sscanf(t->c_str(), "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second);
        NES_DEBUG(" the year {} the month {} the day {} the hour {} the minute {} and the second {}",
                  year,
                  month,
                  day,
                  hour,
                  minute,
                  second);
    } else {
        std::sscanf(t->c_str(), "%d-%d-%d", &year, &month, &day);
        NES_DEBUG(" the year {} the month {} and the day {}", year, month, day);
    }
    //TODO: Currently, we only support this format in a rather naive way, with issue #3616 we want to enhance the format, e.g., by
    // distinguishing Date and Time input strings or support user-defined formats.

    // Create a `std::tm` object with the parsed date and time.
    //time_t tmp = {0};
    struct tm tm = convertToUTC_TM(0L);
    if (year >= 0) {
        tm.tm_year = year - 1900;
    }
    if (month >= 0 && month < 13) {
        tm.tm_mon = month - 1;
    }
    if (day >= 0 && day < 32) {
        tm.tm_mday = day;
    }
    if (hour >= 0 && hour < 25) {
        tm.tm_hour = hour;
    }
    if (minute >= 0 && minute < 60) {
        tm.tm_min = minute;
    }
    if (second >= 0 && second < 60) {
        tm.tm_sec = second;
    }

    // Convert the `std::tm` object to a `std::time_t` object.
    std::time_t time = timegm(&tm);
    // Convert the `std::time_t` object to milliseconds since the Unix epoch.
    std::chrono::duration<int64_t, std::milli> dur(time);
    //has we current only except time formats with hour, minutes and seconds, we need to multiple by 1000 to represent milliseconds
    return dur.count() * 1000;
}

TimeStamp::TimeStamp(Value<> x) : Any(&type), milliseconds(x) {
    if (x->isType<UInt64>()) {
        this->milliseconds = x.as<UInt64>();
    } else if (x->isType<Text>()) {
        this->milliseconds =
            FunctionCall<>("stringtomillisecondsproxy", stringtomillisecondsproxy, x.as<Text>().value->getReference());
    } else {
        NES_THROW_RUNTIME_ERROR("Can not make a TimeStamp object out of" << x);
    }
}

Value<UInt64> TimeStamp::stringtomilliseconds(Value<Text> textValue) {
    return FunctionCall<>("stringtomillisecondsproxy", stringtomillisecondsproxy, textValue->getReference());
}

AnyPtr TimeStamp::copy() { return create<TimeStamp>(milliseconds); }
AnyPtr TimeStamp::add(const TimeStamp& other) const { return create<TimeStamp>(milliseconds + other.milliseconds); }
AnyPtr TimeStamp::substract(const TimeStamp& other) const { return create<TimeStamp>(milliseconds - other.milliseconds); }
std::shared_ptr<Boolean> TimeStamp::equals(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds == otherValue.milliseconds);
}
std::shared_ptr<Boolean> TimeStamp::lessThan(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds < otherValue.milliseconds);
}
std::shared_ptr<Boolean> TimeStamp::greaterThan(const TimeStamp& otherValue) const {
    return create<Boolean>(milliseconds > otherValue.milliseconds);
}

uint64_t centuryProxy(uint64_t milliseconds) {
    // Convert the timestamp to a tm struct.
    tm time = convertToUTC_TM(milliseconds);
    // Extract the year
    int year = time.tm_year + 1900;
    // Divide the year by 100
    return (year / 100) + 1;
}

Value<> TimeStamp::century() { return FunctionCall<>("centuryProxy", centuryProxy, milliseconds.as<UInt64>()); }

TextValue* intervalproxy(int64_t milliseconds) {
    tm time = convertToUTC_TM(milliseconds);
    auto year = time.tm_year + 1900;
    auto month = time.tm_mon;
    auto day = time.tm_mday;
    auto hour = time.tm_hour;
    auto minute = time.tm_min;
    auto seconds = time.tm_sec;
    std::ostringstream interval;
    if (year > 0) {
        interval << year << (year > 1 ? " years " : " year ");
    }
    if (month > 0) {
        interval << month << (month > 1 ? " months " : " month ");
    }
    if (day > 0) {
        interval << day << (day > 1 ? " days " : " day ");
    }
    if (hour > 0) {
        interval << hour << (hour > 1 ? " hours " : " hour ");
    }
    if (minute > 0) {
        interval << minute << (minute > 1 ? " minutes " : " minute ");
    }
    if (seconds > 0) {
        interval << seconds << (seconds > 1 ? " seconds" : " second");
    }
    auto inti = interval.str();
    return NES::Nautilus::TextValue::create(inti);
}
Value<Text> TimeStamp::interval() { return (FunctionCall<>("intervalproxy", intervalproxy, milliseconds.as<Int64>())); }

TextValue* weekdayNameproxy(int64_t milliseconds) {
    // Convert the timestamp to a tm struct.
    tm time = convertToUTC_TM(milliseconds);
    // Extract the day of the week
    int dayOfWeek = time.tm_wday;
    // Use an array of weekday names
    const std::string weekdayNames[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
    // Return the name of the day of the week
    auto weekdayName = weekdayNames[dayOfWeek];
    //Value<TextValue> valueweekdayname = (TextValue)weekdayName);
    NES_TRACE("Find the weekday name {}", weekdayName);
    NES::Nautilus::TextValue* t = NES::Nautilus::TextValue::create(weekdayName);
    return t;
}
Value<Text> TimeStamp::getWeekdayName() {
    return (FunctionCall<>("weekdayNameproxy", weekdayNameproxy, milliseconds.as<Int64>()));
}

uint64_t agecurrentTime(uint64_t milliseconds) {
    //get current timestamp
    auto current =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto age = current - milliseconds;
    return age;
}
Value<> TimeStamp::age() { return FunctionCall<>("agecurrentTime", agecurrentTime, milliseconds.as<UInt64>()); }

Value<> TimeStamp::difference(Value<TimeStamp>& other) {
    if (this->milliseconds.as<UInt64>() > other->milliseconds.as<UInt64>()) {
        return this->milliseconds.as<UInt64>() - other->milliseconds.as<UInt64>();
    } else {
        return other->milliseconds.as<UInt64>() - this->milliseconds.as<UInt64>();
    }
}

Nautilus::IR::Types::StampPtr TimeStamp::getType() const { return Nautilus::IR::Types::StampFactory::createUInt64Stamp(); }

Value<> TimeStamp::getMilliSeconds() { return this->milliseconds; }

int64_t getSecondsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_sec;
}
Value<> TimeStamp::getSeconds() { return FunctionCall<>("getSeconds", getSecondsChrono, milliseconds.as<UInt64>()); }
int64_t getMinutesChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_min;
}
Value<> TimeStamp::getMinutes() { return FunctionCall<>("getMinutes", getMinutesChrono, milliseconds.as<UInt64>()); }
int64_t getHoursChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_hour;
}
Value<> TimeStamp::getHours() { return FunctionCall<>("getHours", getHoursChrono, milliseconds.as<UInt64>()); }
int64_t getDaysChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_mday;
}
Value<> TimeStamp::getDays() { return FunctionCall<>("getDays", getDaysChrono, milliseconds.as<UInt64>()); }
int64_t getMonthsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_mon + 1;
}
Value<> TimeStamp::getMonths() { return FunctionCall<>("getMonths", getMonthsChrono, milliseconds.as<UInt64>()); }
int64_t getYearsChrono(int64_t milliseconds) {
    tm utc_t = convertToUTC_TM(milliseconds);
    return utc_t.tm_year + 1900;
}
Value<> TimeStamp::getYears() { return FunctionCall<>("getYears", getYearsChrono, milliseconds.as<UInt64>()); }

TextValue* getMonthNameProxy(int64_t milliseconds) {
    int64_t month = getMonthsChrono(milliseconds);
    switch (month) {
        case 1: return TextValue::create("January");
        case 2: return TextValue::create("February");
        case 3: return TextValue::create("March");
        case 4: return TextValue::create("April");
        case 5: return TextValue::create("May");
        case 6: return TextValue::create("June");
        case 7: return TextValue::create("July");
        case 8: return TextValue::create("August");
        case 9: return TextValue::create("September");
        case 10: return TextValue::create("October");
        case 11: return TextValue::create("November");
        case 12: return TextValue::create("December");
        default: NES_THROW_RUNTIME_ERROR("Invalid Month");
    }
}

Value<Text> TimeStamp::getMonthName() { return FunctionCall<>("getNameOfMonth", getMonthNameProxy, milliseconds.as<Int64>()); }

Value<> TimeStamp::getValue() { return milliseconds; }
std::string getStringFromMillis(int64_t milliseconds) {
    using time_point = std::chrono::system_clock::time_point;
    time_point milliseconds_timepoint{std::chrono::duration_cast<time_point::duration>(std::chrono::nanoseconds(milliseconds))};
    std::time_t t = std::chrono::system_clock::to_time_t(milliseconds_timepoint);
    return std::ctime(&t);
}
std::string TimeStamp::toString() {
    Value<Text> result = FunctionCall<>("intervalproxy", intervalproxy, milliseconds.as<UInt64>());
    return result->toString();
}

}// namespace NES::Nautilus