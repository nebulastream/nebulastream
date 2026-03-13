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

#include <DistributedQuery.hpp>

#include <array>
#include <ctime>
#include <random>
#include <string>
#include <string_view>
#include <fmt/format.h>

namespace
{
constexpr std::array HORSE_BREEDS = std::to_array<std::string_view>(
    {"arabian",    "thoroughbred", "quarter",  "morgan",       "appaloosa", "paint",     "mustang",   "andalusian", "friesian",
     "clydesdale", "percheron",    "belgian",  "shire",        "shetland",  "welsh",     "connemara", "hanoverian", "warmblood",
     "lipizzaner", "palomino",     "buckskin", "standardbred", "tennessee", "icelandic", "haflinger"});

constexpr std::array ATTRIBUTES = std::to_array<std::string_view>(
    {"swift",    "brave",    "noble",   "wild",        "gallant",   "proud",   "mighty",  "gentle",   "fierce",   "graceful",
     "spirited", "majestic", "elegant", "strong",      "agile",     "loyal",   "bold",    "free",     "dashing",  "swift",
     "valiant",  "fearless", "regal",   "magnificent", "brilliant", "radiant", "blazing", "charging", "prancing", "soaring"});

/// Generate a unique Docker-style name for distributed query identifiers
std::string generateHorseName()
{
    static std::mt19937 rng(static_cast<unsigned>(std::time(nullptr))); /// NOLINT(cert-msc51-cpp)
    std::uniform_int_distribution<> randomBreed(0, HORSE_BREEDS.size() - 1);
    std::uniform_int_distribution<> randomAttribute(0, ATTRIBUTES.size() - 1);

    std::string_view attribute = ATTRIBUTES.at(randomAttribute(rng));
    std::string_view breed = HORSE_BREEDS.at(randomBreed(rng));

    return fmt::format("{}_{}", attribute, breed);
}

}

NES::DistributedQueryId NES::getNextDistributedQueryId()
{
    return NES::DistributedQueryId(generateHorseName());
}
