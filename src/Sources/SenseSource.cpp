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

#include <NodeEngine/QueryManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Util/Logger.hpp>
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
using namespace std;

namespace NES {

SenseSource::SenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& udsf, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId),
      udsf(udsf) {
}

std::optional<TupleBuffer> SenseSource::receiveData() {
    NES_DEBUG("SenseSource::receiveData called");
    auto buf = bufferManager->getBufferBlocking();
    fillBuffer(buf);
    NES_DEBUG("SenseSource::receiveData filled buffer with tuples=" << buf.getNumberOfTuples());
    buf.setWatermark(this->watermark->getWatermark());
    return buf;
}

const std::string SenseSource::toString() const {
    std::stringstream ss;
    ss << "SenseSource(SCHEMA(" << schema->toString() << "), UDSF=" << udsf
       << endl;
    return ss.str();
}

void SenseSource::fillBuffer(TupleBuffer&) {
}

SourceType SenseSource::getType() const {
    return SENSE_SOURCE;
}

const string& SenseSource::getUdsf() const {
    return udsf;
}

}// namespace NES
