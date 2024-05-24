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
#ifdef ENABLE_KAFKA_BUILD
#include <DataGeneration/DataGenerator.hpp>
#include <DataProvider/DataProvider.hpp>
#include <cppkafka/cppkafka.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <unordered_map>

const uint64_t bufferSizeInBytes = 131072;
const uint64_t flushDelay = 131072;

int main(int argc, const char* argv[]) {
    // Activating and installing error listener
    std::cout << "hello" << std::endl;
    //usage --broker --topic --partition
    if (argc != 5) {
        std::cout << "Usage: --broker= --topic= --numberOfPartitions --numberOfBuffersToProduce= required as a command line "
                     "argument!\nExiting now..."
                  << std::endl;
        return -1;
    }

    // Iterating through the arguments
    std::unordered_map<std::string, std::string> argMap;
    for (int i = 0; i < argc; ++i) {
        auto pathArg = std::string(argv[i]);
        if (pathArg.find("--broker") != std::string::npos) {
            argMap["broker"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        } else if (pathArg.find("--topic") != std::string::npos) {
            argMap["topic"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        } else if (pathArg.find("--numberOfPartitions") != std::string::npos) {
            argMap["numberOfPartitions"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        } else if (pathArg.find("--numberOfBuffersToProduce") != std::string::npos) {
            argMap["numberOfBuffersToProduce"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        } else {
            std::cout << "parameter " << pathArg << " not supported" << std::endl;
        }
    }

    uint64_t numberOfBuffersToProduce = std::atoi(argMap["numberOfBuffersToProduce"].c_str());
    uint64_t numberOfPartitions = std::atoi(argMap["numberOfPartitions"].c_str());

    std::cout << "start with config broker=" << argMap["broker"] << " topic=" << argMap["topic"]
              << " numberOfPartitions=" << argMap["numberOfPartitions"]
              << " numberOfBuffersToProduce=" << argMap["numberOfBuffersToProduce"] << std::endl;
    //push data to kafka topic
    cppkafka::Configuration config = {{"metadata.broker.list", argMap["broker"]}};
    cppkafka::Producer producer(config);

    auto bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSizeInBytes, numberOfBuffersToProduce);

    auto dataGenerator = NES::Benchmark::DataGeneration::DataGenerator::createGeneratorByName("YSBKafka", Yaml::Node());
    dataGenerator->setBufferManager(bufferManager);
    auto createdBuffers = dataGenerator->createData(numberOfBuffersToProduce, bufferSizeInBytes);

    std::vector<std::future<void>> futures;
    for (uint64_t partition = 0; partition < numberOfPartitions; partition++) {
        futures.push_back(std::async(std::launch::async, [&, partition]() {
            std::cout << "start producer thread for partition=" << partition << std::endl;
            for (uint64_t prodBuffer = 0; prodBuffer < numberOfBuffersToProduce; prodBuffer++) {
                char* buffer = createdBuffers[prodBuffer].getBuffer<char>();
                size_t len = createdBuffers[prodBuffer].getBufferSize();
                std::vector<uint8_t> bytes(buffer, buffer + len);
                try {
                    producer.produce(cppkafka::MessageBuilder(argMap["topic"]).partition(partition).payload(bytes));
                    producer.flush(std::chrono::seconds(flushDelay));
                } catch (const std::exception& ex) {
                    std::cout << ex.what() << std::endl;
                } catch (const std::string& ex) {
                    std::cout << ex << std::endl;
                } catch (...) {
                }
                if (prodBuffer % 1000 == 0) {
                    std::cout << "number of buffers prod =" << prodBuffer << " for partition=" << partition << std::endl;
                }
            }
        }));
    }

    for (auto& e : futures) {
        e.get();
    }
    producer.flush(std::chrono::seconds(flushDelay));

    return 0;
}
#endif
