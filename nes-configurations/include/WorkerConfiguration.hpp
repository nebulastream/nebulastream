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

#ifndef WORKERCONFIGURATIONS_HPP
#define WORKERCONFIGURATIONS_HPP

#include <string>
#include <memory>
#include <ConfigurationManager.hpp>
#include <Validation/IpValidation.hpp>
#include <Validation/NumberValidation.hpp>
#include <Validation/NonZeroValidation.hpp>

enum class CompilationStrategy { FAST, DEBUG, OPTIMIZE };
enum class DumpMode { NONE, CONSOLE, FILE, FILE_AND_CONSOLE };
enum class NautilusBackend { INTERPRETER, MLIR_COMPILER_BACKEND };

class NetworkConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        hostName = configManager.getValue<std::string>("localWorkerHost", "127.0.0.1");
        rpcPort = configManager.getValue<uint64_t>("rpcPort", 0);

        if (!IpValidation::isValid(hostName)) {
            throw std::runtime_error("Invalid IP address: " + hostName);
        }

        if (!NumberValidation::isValid(rpcPort)) {
            throw std::runtime_error("RPC port must be a valid number.");
        }
    }

    std::string getHostName() const { return hostName; }
    uint64_t getRpcPort() const { return rpcPort; }

private:
    std::string hostName;
    uint64_t rpcPort;
};

class QueryManagerConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        numberOfBuffersPerWorker = configManager.getValue<uint64_t>("numberOfBuffersPerWorker", 128);
        numberOfBuffersInSourceLocalBufferPool = configManager.getValue<uint64_t>("numberOfBuffersInSourceLocalBufferPool", 64);
        numWorkerThreads = configManager.getValue<uint64_t>("numWorkerThreads", 1);
        bufferSizeInBytes = configManager.getValue<uint64_t>("bufferSizeInBytes", 4096);
        numberOfBuffersInGlobalBufferManager = configManager.getValue<uint64_t>("numberOfBuffersInGlobalBufferManager", 1024);

        if (!NonZeroValidation::isValid(numberOfBuffersPerWorker)) {
            throw std::runtime_error("Number of buffers per worker must be a valid number.");
        }

        if (!NonZeroValidation::isValid(numberOfBuffersInSourceLocalBufferPool)) {
            throw std::runtime_error("Number of buffers in source local buffer pool must be a valid number.");
        }

        if (!NonZeroValidation::isValid(numWorkerThreads)) {
            throw std::runtime_error("Number of worker threads must be a valid number.");
        }

        if (!NonZeroValidation::isValid(bufferSizeInBytes)) {
            throw std::runtime_error("Buffer size in bytes must be a valid number.");
        }

        if (!NonZeroValidation::isValid(numberOfBuffersInGlobalBufferManager)) {
            throw std::runtime_error("Number of buffers in global buffer manager must be a valid number.");
        }
    }

    uint64_t getNumberOfBuffersPerWorker() const { return numberOfBuffersPerWorker; }
    uint64_t getNumberOfBuffersInSourceLocalBufferPool() const { return numberOfBuffersInSourceLocalBufferPool; }
    uint64_t getNumWorkerThreads() const { return numWorkerThreads; }
    uint64_t getBufferSizeInBytes() const { return bufferSizeInBytes; }
    uint64_t getNumberOfBuffersInGlobalBufferManager() const { return numberOfBuffersInGlobalBufferManager; }

private:
    uint64_t numberOfBuffersPerWorker;
    uint64_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t numWorkerThreads;
    uint64_t bufferSizeInBytes;
    uint64_t numberOfBuffersInGlobalBufferManager;
};

class LoggingConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        logLevel = configManager.getValue<std::string>("logLevel", "LOG_INFO");
    }

    std::string getLogLevel() const { return logLevel; }

private:
    std::string logLevel;
};

class QueryCompilerConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        compilationStrategy = configManager.getValue<CompilationStrategy>("compilationStrategy", CompilationStrategy::OPTIMIZE);
        dumpMode = configManager.getValue<DumpMode>("dumpMode", DumpMode::NONE);
        nautilusBackend = configManager.getValue<NautilusBackend>("nautilusBackend", NautilusBackend::MLIR_COMPILER_BACKEND);
    }

    CompilationStrategy getCompilationStrategy() const { return compilationStrategy; }
    DumpMode getDumpMode() const { return dumpMode; }
    NautilusBackend getNautilusBackend() const { return nautilusBackend; }

private:
    CompilationStrategy compilationStrategy;
    DumpMode dumpMode;
    NautilusBackend nautilusBackend;
};

class GeneralConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        configPath = configManager.getValue<std::string>("configPath", "");
        workerId = configManager.getValue<std::string>("workerId", 0);
    }

    std::string getConfigPath() const { return configPath; }
    std::string getWorkerId() const { return workerId; }

private:
    std::string configPath;
    std::string workerId;
};

#endif // WORKERCONFIGURATIONS_HPP

