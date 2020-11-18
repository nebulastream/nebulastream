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

#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, "TEXT_FORMAT", false));
}

SinkDescriptorPtr FileSinkDescriptor::create(std::string fileName, std::string sinkFormat, std::string append) {
    return std::make_shared<FileSinkDescriptor>(FileSinkDescriptor(fileName, sinkFormat, append == "APPEND" ? true : false));
}

FileSinkDescriptor::FileSinkDescriptor(std::string fileName, std::string sinkFormat, bool append)
    : fileName(fileName), sinkFormat(sinkFormat), append(append) {}

const std::string& FileSinkDescriptor::getFileName() const { return fileName; }

std::string FileSinkDescriptor::toString() { return "FileSinkDescriptor()"; }

bool FileSinkDescriptor::equal(SinkDescriptorPtr other) {
    if (!other->instanceOf<FileSinkDescriptor>())
        return false;
    auto otherSinkDescriptor = other->as<FileSinkDescriptor>();
    return fileName == otherSinkDescriptor->fileName;
}

bool FileSinkDescriptor::getAppend() { return append; }

std::string FileSinkDescriptor::getSinkFormatAsString() { return sinkFormat; }

}// namespace NES