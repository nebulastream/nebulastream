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
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionPath.hpp>

namespace NES::Nautilus::Tracing {
void SymbolicExecutionPath::append(bool outcome, Tag& tag) { path.emplace_back(std::make_tuple(outcome, tag)); }

std::tuple<bool, Tag> SymbolicExecutionPath::operator[](uint64_t size) { return path[size]; }

uint64_t SymbolicExecutionPath::getSize() { return path.size(); }
std::ostream& operator<<(std::ostream& os, const SymbolicExecutionPath& path) {
    os << "[";
    for (auto p : path.path) {
        os << std::get<0>(p) << ",";
    }
    os << "]";
    return os;
};

std::vector<std::tuple<bool, Tag>>& SymbolicExecutionPath::getPath() { return path; }

}// namespace NES::Nautilus::Tracing