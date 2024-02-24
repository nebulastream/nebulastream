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

#include <Runtime/NodeEngine.hpp>
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
//constexpr bool WRITE_ALL_ON_SHUTDOWN = true;

SinkMediumTypes FileSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

FileSink::FileSink(SinkFormatPtr format,
                   Runtime::NodeEnginePtr nodeEngine,
                   uint32_t numOfProducers,
                   const std::string& filePath,
                   bool append,
                   QueryId queryId,
                   DecomposedQueryPlanId querySubPlanId,
                   uint64_t numberOfOrigins,
                   bool timestampAndWriteToSocket)
    : SinkMedium(std::move(format), std::move(nodeEngine), numOfProducers, queryId, querySubPlanId, numberOfOrigins),
      timestampAndWriteToSocket(timestampAndWriteToSocket) {
    this->filePath = filePath;
    this->append = append;
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            bool success = std::filesystem::remove(filePath.c_str());
            NES_ASSERT2_FMT(success, "cannot remove file " << filePath.c_str());
        }
    }
    NES_DEBUG("FileSink: open file= {}", filePath);

    if (!timestampAndWriteToSocket) {
        // open the file stream
        if (!outputFile.is_open()) {
            outputFile.open(filePath, std::ofstream::binary | std::ofstream::app);
        }
        NES_ASSERT(outputFile.is_open(), "file is not open");
        NES_ASSERT(outputFile.good(), "file not good");
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

        NES_INFO("Connecting to tcp socket")
        if (!this->nodeEngine->getTcpDescriptor().has_value()) {
            NES_INFO("No existing tcp descriptor, opening one now")

            auto port = std::stoi(filePath);

            // Create a TCP socket
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd == -1) {
                perror("socket");
                return;
            }

            // Specify the address and port of the server
            struct sockaddr_in server_addr;
            memset(&server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");// Example IP address
            server_addr.sin_port = htons(port);                 // Example port number

            // Connect to the server
            if (connect(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
                perror("connect");
                close(sockfd);
                return;
            }
            this->nodeEngine->setTcpDescriptor(sockfd);
        } else {
            NES_INFO("Found existing tcp descriptor")
            sockfd = this->nodeEngine->getTcpDescriptor().value();
        }
    }
    NES_INFO("Successfully connected")
}

FileSink::~FileSink() {
    NES_DEBUG("~FileSink: close file={}", filePath);
    //outputFile.close();
    //close(sockfd);
}

std::string FileSink::toString() const {
    std::stringstream ss;
    ss << "FileSink(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void FileSink::setup() {}

void FileSink::shutdown() {
    if (timestampAndWriteToSocket) {
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
    if (timestampAndWriteToSocket) {
        std::unique_lock lock(writeMutex);
//        std::string bufferContent;
        //auto schema = sinkFormat->getSchemaPtr();
        //schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        //bufferContent = Util::printTupleBufferAsCSV(inputBuffer, schema, timestampAndWriteToSocket);
        //totalTupleCountreceived += inputBuffer.getNumberOfTuples();


        // Write data to the socket
        //ssize_t bytes_written = write(sockfd, bufferContent.c_str(), bufferContent.length());

        auto* records = inputBuffer.getBuffer<Record>();
        for (uint64_t i = 0; i < inputBuffer.getNumberOfTuples(); ++i) {
            records[i].outputTimestamp = getTimestamp();
        }
        ssize_t bytes_written = write(sockfd, inputBuffer.getBuffer(), inputBuffer.getBufferSize());
        if (bytes_written == -1) {
            perror("write");
            close(sockfd);
            return 1;
        }

        //        receivedBuffers.push_back(bufferContent);
        //        arrivalTimestamps.push_back(getTimestamp());

        return true;
    }
    return writeDataToFile(inputBuffer);
}

std::string FileSink::getFilePath() const { return filePath; }

bool FileSink::writeDataToFile(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);
    NES_DEBUG("FileSink: getSchema medium {} format {} mode {}", toString(), sinkFormat->toString(), this->getAppendAsString());

    if (!inputBuffer) {
        NES_ERROR("FileSink::writeDataToFile input buffer invalid");
        return false;
    }

    if (!schemaWritten && sinkFormat->getSinkFormat() != FormatTypes::NES_FORMAT) {
        auto schemaStr = sinkFormat->getFormattedSchema();
        outputFile.write(schemaStr.c_str(), (int64_t) schemaStr.length());
        schemaWritten = true;
    } else if (sinkFormat->getSinkFormat() == FormatTypes::NES_FORMAT) {
        NES_DEBUG("FileSink::getData: writing schema skipped, not supported for NES_FORMAT");
    } else {
        NES_DEBUG("FileSink::getData: schema already written");
    }

    auto fBuffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_DEBUG("FileSink::getData: writing to file {} following content {}", filePath, fBuffer);
    outputFile.write(fBuffer.c_str(), fBuffer.size());
    outputFile.flush();
    return true;
}

bool FileSink::getAppend() const { return append; }

std::string FileSink::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

}// namespace NES
