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

#include <string>
#include <Sinks/Sink.hpp>

namespace NES::Sinks
{

std::ostream& operator<<(std::ostream& out, const Sink& sink)
{
    return sink.toString(out);
}
bool operator==(const Sink& lhs, const Sink& rhs)
{
    return lhs.equals(rhs);
}

}
