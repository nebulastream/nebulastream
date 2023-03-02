#include <Util/TestHarness/TestHarnessWorkerConfiguration.hpp>

namespace NES {

TestHarnessWorkerConfigurationPtr TestHarnessWorkerConfiguration::create(WorkerConfigurationPtr workerConfiguration,
                                                                         uint32_t workerId) {
    return std::make_shared<TestHarnessWorkerConfiguration>(
        TestHarnessWorkerConfiguration(std::move(workerConfiguration), NonSource, workerId));
}

TestHarnessWorkerConfigurationPtr TestHarnessWorkerConfiguration::create(WorkerConfigurationPtr workerConfiguration,
                                                                         std::string logicalSourceName,
                                                                         std::string physicalSourceName,
                                                                         TestHarnessWorkerSourceType sourceType,
                                                                         uint32_t workerId) {
    return std::make_shared<TestHarnessWorkerConfiguration>(TestHarnessWorkerConfiguration(std::move(workerConfiguration),
                                                                                           std::move(logicalSourceName),
                                                                                           std::move(physicalSourceName),
                                                                                           sourceType,
                                                                                           workerId));
}

TestHarnessWorkerConfiguration::TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                                               TestHarnessWorkerSourceType sourceType,
                                                               uint32_t workerId)
    : workerConfiguration(std::move(workerConfiguration)), sourceType(sourceType), workerId(workerId){};

TestHarnessWorkerConfiguration::TestHarnessWorkerConfiguration(WorkerConfigurationPtr workerConfiguration,
                                                               std::string logicalSourceName,
                                                               std::string physicalSourceName,
                                                               TestHarnessWorkerSourceType sourceType,
                                                               uint32_t workerId)
    : workerConfiguration(std::move(workerConfiguration)), logicalSourceName(std::move(logicalSourceName)),
      physicalSourceName(std::move(physicalSourceName)), sourceType(sourceType), workerId(workerId){};

void TestHarnessWorkerConfiguration::setQueryStatusListener(const NesWorkerPtr& nesWorker) { this->nesWorker = nesWorker; }

PhysicalSourceTypePtr TestHarnessWorkerConfiguration::getPhysicalSourceType() const { return physicalSource; }

const WorkerConfigurationPtr& TestHarnessWorkerConfiguration::getWorkerConfiguration() const { return workerConfiguration; }

TestHarnessWorkerConfiguration::TestHarnessWorkerSourceType TestHarnessWorkerConfiguration::getSourceType() const {
    return sourceType;
}
void TestHarnessWorkerConfiguration::setPhysicalSourceType(PhysicalSourceTypePtr physicalSource) {
    this->physicalSource = physicalSource;
}
const std::vector<uint8_t*>& TestHarnessWorkerConfiguration::getRecords() const { return records; }

void TestHarnessWorkerConfiguration::addRecord(uint8_t* record) { records.push_back(record); }

uint32_t TestHarnessWorkerConfiguration::getWorkerId() const { return workerId; }

const std::string& TestHarnessWorkerConfiguration::getLogicalSourceName() const { return logicalSourceName; }

const std::string& TestHarnessWorkerConfiguration::getPhysicalSourceName() const { return physicalSourceName; }
const NesWorkerPtr& TestHarnessWorkerConfiguration::getNesWorker() const { return nesWorker; }
}// namespace NES