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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
#include <API/TimeUnit.hpp>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>

namespace NES::QueryCompilation {
class TimestampField {
  public:
    friend std::ostream& operator<<(std::ostream& os, const TimestampField& obj) {
        return os << fmt::format("TimestampField({}, {})", obj.fieldName, obj.unit.getMultiplier());
    }
    [[nodiscard]] Windowing::TimeUnit getUnit() const { return unit; }
    [[nodiscard]] const std::string& getName() const { return fieldName; }
    [[nodiscard]] bool isIngestionTime() const { return ingestionTime; }
    static TimestampField IngestionTime() { return {"IngestionTime", Windowing::TimeUnit(1), true}; }
    static TimestampField EventTime(std::string fieldName, Windowing::TimeUnit tm) {
        return {std::move(fieldName), std::move(tm), false};
    }

  private:
    std::string fieldName;
    Windowing::TimeUnit unit;
    bool ingestionTime;
    TimestampField(std::string fieldName, Windowing::TimeUnit unit, bool ingestionTime)
        : fieldName(std::move(fieldName)), unit(std::move(unit)), ingestionTime(ingestionTime) {}
};
}// namespace NES::QueryCompilation

#endif//NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
