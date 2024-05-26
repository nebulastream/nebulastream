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

#include <Nautilus/Interface/DataTypes/Text/RawStringView.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Common.hpp>
#include <cstring>

namespace NES::Nautilus {

bool rawStringViewEmptyProxy(std::string_view view) { return view.empty(); }
RawStringView::RawStringView(std::string_view view) : Any(&type), begin(view.data()), end(view.data() + view.size()) {}
}// namespace NES::Nautilus
