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


#pragma once
#include <atomic>
#include <cstddef>
#include <ostream>
#include <string_view>

struct Checksum
{
    /// Position within the formatted rows of a changelog, which can be split across the chunks passed to addChangelog.
    struct ChangelogPosition
    {
        bool atLineStart = true;
        bool inKind = false;
        long sign = 1;
    };

    void add(std::string_view data);
    /// Adds rows whose first column is the row kind as +I, -U, +U or -D (see RowKind.hpp). The kind column is not part of the checksum, and rows that
    /// withdraw from the final table (UPDATE_BEFORE, DELETE) subtract themselves, so the result is the checksum of the final table.
    void addChangelog(std::string_view data, ChangelogPosition& position);
    bool operator==(const Checksum&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const Checksum& obj);
    std::atomic<size_t> checksum = 0;
    std::atomic<size_t> numberOfTuples = 0;
};
