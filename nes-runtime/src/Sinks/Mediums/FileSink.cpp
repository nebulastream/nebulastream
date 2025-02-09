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

#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <arpa/inet.h>
#include <filesystem>
#include <iostream>
#include <netinet/in.h>
#include <regex>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <utility>

namespace NES {

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   SharedQueryId sharedQueryId,
                   DecomposedQueryId decomposedQueryId,
                   DecomposedQueryPlanVersion decomposedQueryVersion,
                   uint64_t numberOfOrigins,
                   bool timestampAndWriteToSocket)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 sharedQueryId,
                 decomposedQueryId,
                 decomposedQueryVersion,
                 numberOfOrigins),
      filePath(filePath), append(append), timestampAndWriteToSocket(true) {
    (void) timestampAndWriteToSocket;
}

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {
    NES_DEBUG("Setting up file sink; filePath={}, schema={}, sinkFormat={}, append={}",
              filePath,
              sinkFormat->getSchemaPtr()->toString(),
              sinkFormat->toString(),
              append);
    // Remove an existing file unless the append mode is APPEND.
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath={} ", filePath);
                isOpen = false;
                return;
            }
        }
    } else {
        if (std::filesystem::exists(filePath.c_str())) {
            // Skip writing the schema to the file as we are appending to an existing file.
            schemaWritten = true;
        }
    }

if (!timestampAndWriteToSocket) {
    // Open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFile.is_open() && outputFile.good();
    if (!isOpen) {
        NES_ERROR("Could not open output file; filePath={}, is_open()={}, good={}",
                  filePath,
                  outputFile.is_open(),
                  outputFile.good());
    }

} else {

    //todo: use this in case domain socket should be used instead of tcp
    //    // Create a Unix domain socket
    //    clientSockFd = socket(AF_UNIX, SOCK_STREAM, 0);
    //    if (clientSockFd == -1) {
    //        perror("socket");
    //        return;
    //    }
    //
    //    // Set up the address structure
    //    struct sockaddr_un addr;
    //    memset(&addr, 0, sizeof(addr));
    //    addr.sun_family = AF_UNIX;
    //    strncpy(addr.sun_path, filePath.c_str(), sizeof(addr.sun_path) - 1);
    //
    //    // Connect to the Unix domain socket
    //    if (connect(clientSockFd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
    //        perror("connect");
    //        close(clientSockFd);
    //        return;
    //    }

    NES_DEBUG("Connecting to tcp socket on worker {}", nodeEngine->getNodeId());
    //todo: this will not work for multiple sinks
//    if (!this->nodeEngine->getTcpDescriptor(filePath).has_value()) {
////    if (true) {
//    } else {
//        sockfd = this->nodeEngine->getTcpDescriptor(filePath).value();
//        NES_ERROR("Found existing tcp descriptor {} for {}", sockfd, filePath)
//    }
    //get it once
    NES_DEBUG("creating socket for file sink")
        auto sockfd = nodeEngine->getTcpDescriptor(filePath);
}
}

void FileSink::shutdown() {
    if (timestampAndWriteToSocket) {
        //todo: send checkpoint message
        NES_ERROR("sink notifies checkpoints");
        auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
        auto checkpoints = sinkInfo->checkpoints;
        nodeEngine->notifyCheckpoints(sharedQueryId, checkpoints);
        //NES_INFO("total buffers received at file sink {}", totalTupleCountreceived);
        //        for (const auto& bufferContent : receivedBuffers) {
        //            outputFile.write(bufferContent.c_str(), bufferContent.size());
        //        }
        //outputFile.flush();
    }
}

struct Record {
    uint64_t id;
    uint64_t value;
    uint64_t ingestionTimestamp;
    uint64_t processingTimestamp;
    uint64_t outputTimestamp;
};

bool FileSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    // auto sockfd = nodeEngine->getTcpDescriptor(filePath);
    auto sinkInfo = nodeEngine->getTcpDescriptor(filePath);
    if (timestampAndWriteToSocket) {
        NES_DEBUG("write data to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
        std::unique_lock lock(writeMutex);
//        std::string bufferContent;
        //auto schema = sinkFormat->getSchemaPtr();
        //schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        //bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema, timestampAndWriteToSocket);
        //totalTupleCountreceived += inputBuffer.getNumberOfTuples();


        // Write data to the socket
        //ssize_t bytes_written = write(sockfd, bufferContent.c_str(), bufferContent.length());

        auto* records = inputBuffer.getBuffer<Record>();
        //todo: do not checkpoint if incremental is enabled
        for (uint64_t i = 0; i < inputBuffer.getNumberOfTuples(); ++i) {
            records[i].outputTimestamp = getTimestamp();
            auto& checkpoint = sinkInfo->checkpoints[records[i].id];
            if (checkpoint != 0 && checkpoint >= records[i].value) {
                NES_ERROR("checkpoint is {}, tuple is {}. Replay cannot work with out of orderness", checkpoint, records[i].value);
                NES_ASSERT(false, "Checkpoints not increasing");
            }
            checkpoint = records[i].value;
        }
//        NES_ERROR("Writing to tcp sink");
        ssize_t bytes_written = write(sinkInfo->sockfd, inputBuffer.getBuffer(), inputBuffer.getNumberOfTuples() * sizeof(Record));
//        NES_ERROR("{} bytes written to tcp sink", bytes_written);
        if (bytes_written == -1) {
            NES_ERROR("Could not write to socket in sink")
            perror("write");
            close(sinkInfo->sockfd);
            return 1;
        }

        //        receivedBuffers.push_back(bufferContent);
        //        arrivalTimestamps.push_back(getTimestamp());

        NES_DEBUG("finished writing to sink with descriptor {} for {}", sinkInfo->sockfd, filePath)
        return true;
    }
    return writeDataToFile(inputBuffer);
}

std::string FileSink::getFilePath() const { return filePath; }

bool FileSink::writeDataToFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        NES_ERROR("Invalid input buffer");
        return false;
    }

    if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing schema to file sink; filePath = {}, schema = {}, sinkFormat = {}",
                  filePath,
                  sinkFormat->getSchemaPtr()->toString(),
                  sinkFormat->toString());
        auto schemaStr = sinkFormat->getFormattedSchema();
        outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
        schemaWritten = true;
    } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
        NES_DEBUG("Writing the schema is not supported for NES_FORMAT");
    } else {
        NES_DEBUG("Schema already written");
    }

    auto fBuffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_DEBUG("Writing tuples to file sink; decomposed query id={} filePath={}, fBuffer={}",
              decomposedQueryId,
              filePath,
              fBuffer);
    outputFile.write(fBuffer.c_str(), fBuffer.size());
    outputFile.flush();
    return true;
}

}// namespace NES
