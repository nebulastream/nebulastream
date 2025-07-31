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

#include <ostream>
#include <fmt/ostream.h>
#include <BackpressureChannel.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES::Sinks
{

class Sink : public ExecutablePipelineStage
{
public:
    Sink(Valve valve) : valve(std::move(valve)) { }
    ~Sink() override = default;
    friend std::ostream& operator<<(std::ostream& out, const Sink& sink);

    Valve valve;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::Sink> : ostream_formatter
{
};
}
