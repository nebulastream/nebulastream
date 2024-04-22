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

#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <utility>

namespace NES {

SinkDescriptorPtr FileSinkDescriptor::create(std::string_view fileName) { return create(fileName, false); }

SinkDescriptorPtr FileSinkDescriptor::create(std::string_view fileName, bool addTimestamp) {
    return create(fileName, "CSV_FORMAT", "OVERWRITE", addTimestamp);
}

SinkDescriptorPtr
FileSinkDescriptor::create(std::string_view fileName, std::string_view sinkFormat, std::string_view append, bool addTimestamp) {
    return create(fileName, sinkFormat, append, addTimestamp, 1);
}

SinkDescriptorPtr FileSinkDescriptor::create(std::string_view fileName,
                                             std::string_view sinkFormat,
                                             std::string_view append,
                                             bool addTimestamp,
                                             uint64_t numberOfOrigins) {
    return std::make_shared<FileSinkDescriptor>(
        FileSinkDescriptor(fileName, sinkFormat, append == "APPEND",
                                                                   addTimestamp,
                                                                   numberOfOrigins));
}

SinkDescriptorPtr
FileSinkDescriptor::create(const std::string& fileName, const std::string& sinkFormat, const std::string& append) {
    return create(fileName, sinkFormat, append, false, 1);
}

FileSinkDescriptor::FileSinkDescriptor(std::string_view fileName,
                                       std::string_view sinkFormat,
                                       bool append,
                                       bool addTimestamp,
                                       uint64_t numberOfOrigins)
    : SinkDescriptor(numberOfOrigins, addTimestamp), fileName(fileName), sinkFormat(sinkFormat), append(append) {}

const std::string& FileSinkDescriptor::getFileName() const { return fileName; }

std::string FileSinkDescriptor::toString() const { return "FileSinkDescriptor()"; }

bool FileSinkDescriptor::equal(SinkDescriptorPtr const& other) {
    if (!other->instanceOf<FileSinkDescriptor>()) {
        return false;
    }
    auto otherSinkDescriptor = other->as<FileSinkDescriptor>();
    return fileName == otherSinkDescriptor->fileName;
}

bool FileSinkDescriptor::getAppend() const { return append; }

std::string FileSinkDescriptor::getSinkFormatAsString() const { return sinkFormat; }

}// namespace NES