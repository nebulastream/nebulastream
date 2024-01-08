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

#include <cmath>
#include <vector>

#include <Statistics/CountMin.hpp>

namespace NES::Experimental::Statistics {

double CountMin::calcError(const uint64_t width) { return 1.0 / (double) width; }

double CountMin::calcProbability(const uint64_t depth) { return exp(-1.0 * (double) depth); }

const std::vector<std::vector<uint64_t>>& CountMin::getData() const { return data; }

uint64_t CountMin::getWidth() const { return width; }

double CountMin::getError() const { return error; }

double CountMin::getProbability() const { return probability; }
}