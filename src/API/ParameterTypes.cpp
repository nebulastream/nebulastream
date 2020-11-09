/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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


#include <API/ParameterTypes.hpp>
#include <string>

namespace NES {

const DataSourcePtr copy(const DataSourcePtr param) { return param; }
const DataSinkPtr copy(const DataSinkPtr param) { return param; }
const PredicatePtr copy(const PredicatePtr param) { return param; }
const Attributes copy(const Attributes& param) { return param; }
const MapperPtr copy(const MapperPtr param) { return param; }
const std::string toString(const DataSourcePtr) { return "***"; }
const std::string toString(const DataSinkPtr) { return "***"; }
const std::string toString(const PredicatePtr) { return "***"; }
const std::string toString(const Attributes&) { return "***"; }
const std::string toString(const MapperPtr) { return "***"; }

}// namespace NES
