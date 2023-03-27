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

#include <Benchmarking/Parsing/SynopsisArguments.hpp>

namespace NES::ASP {

SynopsisArguments::SynopsisArguments(Type type, size_t width, size_t height, size_t windowSize)
    : type(type), width(width), height(height), windowSize(windowSize) {}

SynopsisArguments SynopsisArguments::createArguments(Type type, size_t width, size_t height,
                                                     size_t windowSize) {
    return SynopsisArguments(type, width, height, windowSize);
}

size_t SynopsisArguments::getWidth() const { return width; }

size_t SynopsisArguments::getHeight() const { return height; }

size_t SynopsisArguments::getWindowSize() const { return windowSize; }

SynopsisArguments::Type SynopsisArguments::getType() const { return type; }

SynopsisArguments SynopsisArguments::createArgumentsFromYamlNode(const Yaml::Node& synopsisArgumentsNode) {


}


} // namespace NES::ASP