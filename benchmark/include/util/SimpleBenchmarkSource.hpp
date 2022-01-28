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

#ifndef NES_BENCHMARK_INCLUDE_UTIL_SIMPLE_BENCHMARK_SOURCE_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_SIMPLE_BENCHMARK_SOURCE_HPP_

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstdint>
#include <list>
#include <memory>

#if __linux
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <sys/syscall.h>
#endif

namespace NES::Benchmarking {
/**
 * @brief A benchmark source that will output data (key, value) with a predefined selectivity
 * Key is an uniform distribution from 0 to 999
 * Value is always 1
 * Selectivity can be set
 */
class SimpleBenchmarkSource : public DataSource {
  public:
    uint64_t numberOfTuplesPerBuffer;

    SimpleBenchmarkSource(const SchemaPtr& schema,
                          const Runtime::BufferManagerPtr& bufferManager,
                          const Runtime::QueryManagerPtr& queryManager,
                          uint64_t ingestionRate,
                          uint64_t numberOfTuplesPerBuffer,
                          uint64_t operatorId)

        : DataSource(schema, bufferManager, queryManager, operatorId, 12, DataSource::GatheringMode::FREQUENCY_MODE) {
        NES_DEBUG("SimpleBenchmarkSource: " << this << " created!");
        this->ingestionRate = ingestionRate;
        this->numberOfTuplesPerBuffer = numberOfTuplesPerBuffer;
        this->rowLayout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, bufferManager->getBufferSize());
        this->curNumberOfTuplesPerBuffer = this->numberOfTuplesPerBuffer;
        this->maxNumberOfPeriods =
            std::ceil((double) BenchmarkUtils::runSingleExperimentSeconds / (double) BenchmarkUtils::periodLengthInSeconds);
        BenchmarkUtils::createUniformData(keyList, curNumberOfTuplesPerBuffer);
    }

    /**
     * @brief this function is very similar to DataSource.cpp runningRoutine(). The difference is that the sleep is in ms
     */
    void runningRoutine() override {
        if (!queryManager) {
            NES_ERROR("query Manager not set");
            throw std::logic_error("SimpleBenchmarkSource: QueryManager not set");
        }
        if (!bufferManager) {
            NES_ERROR("bufferManager not set");
            throw std::logic_error("SimpleBenchmarkSource: BufferManager not set");
        }

        printPIDandParentID;

        if (this->operatorId != 0) {
            NES_DEBUG("SimpleBenchmarkSource " << this->getOperatorId() << ": SimpleBenchmarkSource of type=" << getType());
            uint64_t numberOfTuplesPerPeriod = (ingestionRate * BenchmarkUtils::periodLengthInSeconds);

            NES_DEBUG("SimpleBenchmarkSource: "
                      << "ingestionRate * periodLengthInSeconds = " << ingestionRate * BenchmarkUtils::periodLengthInSeconds
                      << "\nnumberOfTuplesPerBuffer = " << numberOfTuplesPerBuffer);

            auto optBuf = receiveData();
            uint64_t nextPeriodStartTime = 0;
            uint64_t curPeriod = 0;
            while ((curPeriod++) < maxNumberOfPeriods && isRunning()) {
                auto startTimeSendBuffers =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                uint64_t cntTuples = 0;
                while (cntTuples < numberOfTuplesPerPeriod && isRunning()) {

                    //if ((numberOfTuplesPerPeriod - cntTuples) < numberOfTuplesPerBuffer) curNumberOfTuplesPerBuffer = numberOfTuplesPerPeriod - cntTuples;
                    //else curNumberOfTuplesPerBuffer = numberOfTuplesPerBuffer;
                    NES_DEBUG("SimpleBenchmarkSource: curNumberOfTuplesPerBuffer = " << curNumberOfTuplesPerBuffer);

                    // we are using always the same buffer, so no receiveData() call for every iteration
                    // auto optBuf = receiveData();
                    if (optBuf.has_value()) {
                        // here we got a valid buffer
                        auto& buf = optBuf.value();
                        queryManager->addWork(this->operatorId, buf);

                        cntTuples += curNumberOfTuplesPerBuffer;
                    }
                    NES_DEBUG("SimpleBenchmarkSource: cntTuples=" << cntTuples
                                                                  << " numberOfTuplesPerPeriod=" << numberOfTuplesPerPeriod);
                }

                uint64_t endTimeSendBuffers =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();

                nextPeriodStartTime = uint64_t(startTimeSendBuffers + (BenchmarkUtils::periodLengthInSeconds * 1000));
                NES_DEBUG("SimpleBenchmarkSource:\n-startTimeSendBuffers=\t" << startTimeSendBuffers << "\n-endTimeSendBuffers=\t"
                                                                             << endTimeSendBuffers << "\n-nextPeriodStartTime=\t"
                                                                             << nextPeriodStartTime);

                if (nextPeriodStartTime < endTimeSendBuffers) {
                    NES_ERROR("Creating buffer(s) for SimpleBenchmarkSource took longer than periodLength. nextPeriodStartTime="
                              << nextPeriodStartTime << " endTimeSendBuffers=" << endTimeSendBuffers);
                    //throw RuntimeException("Creating buffer(s) for SimpleBenchmarkSource took longer than periodLength!!!");
                }

                uint64_t curTime =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                while (curTime < nextPeriodStartTime) {
                    curTime =
                        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                            .count();
                }
                NES_WARNING("SimpleBenchmarkSource: Done with period " << curPeriod << " of " << maxNumberOfPeriods);
            }
            NES_INFO("SimpleBenchmarkSource: Source is not running anymore or is done with periods!");

            // inject reconfiguration task containing end of stream
            queryManager->addEndOfStream(operatorId, wasGracefullyStopped);//
            bufferManager.reset();
            queryManager.reset();
            NES_DEBUG("DataSource " << operatorId << " end running");
        } else {
            NES_FATAL_ERROR("No Source for Sink detected!!!");
        }//end of if source not empty
    }

    std::optional<Runtime::TupleBuffer> receiveData() override {

        // 10 tuples of size one
        NES_DEBUG("Source:" << this << " requesting buffer");

        auto buf = this->bufferManager->getBufferBlocking();
        auto listIt = keyList.begin();
        std::advance(listIt, keyPos);

        auto bindedRowLayout = rowLayout->bind(buf);

        auto fields = schema->fields;
        for (uint64_t recordIndex = 0; recordIndex < curNumberOfTuplesPerBuffer; recordIndex++) {
            for (uint64_t fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                auto value = *listIt;
                auto dataType = fields[fieldIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                if (physicalType->isBasicType()) {
                    auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                    if (basicPhysicalType->getNativeType() == BasicPhysicalType::CHAR) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<char, false>::create(fieldIndex,
                                                                                                 bindedRowLayout)[recordIndex] =
                            value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_8) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint8_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_16) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint16_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_32) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint32_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_8) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int8_t, false>::create(fieldIndex,
                                                                                                   bindedRowLayout)[recordIndex] =
                            value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_16) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int16_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_32) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int32_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<int64_t, false>::create(
                            fieldIndex,
                            bindedRowLayout)[recordIndex] = value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::FLOAT) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<float, false>::create(fieldIndex,
                                                                                                  bindedRowLayout)[recordIndex] =
                            value;
                    } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::DOUBLE) {
                        Runtime::DynamicMemoryLayout::DynamicRowLayoutField<double, false>::create(fieldIndex,
                                                                                                   bindedRowLayout)[recordIndex] =
                            value;
                    } else {
                        NES_DEBUG("This data source only generates data for numeric fields");
                    }
                } else {
                    NES_DEBUG("This data source only generates data for numeric fields");
                }
            }
        }

        this->keyPos += curNumberOfTuplesPerBuffer;
        buf.setNumberOfTuples(curNumberOfTuplesPerBuffer);

        NES_DEBUG("SimpleBenchmarkSource: available buffer after creating one buffer are "
                  << bufferManager->getAvailableBuffers());
        return buf;
    }

    const std::string toString() const override { return "SimpleBenchmarkSource"; }

    SourceType getType() const override { return TEST_SOURCE; }

    virtual ~SimpleBenchmarkSource() = default;

    static std::shared_ptr<SimpleBenchmarkSource> create(Runtime::BufferManagerPtr bufferManager,
                                                         Runtime::QueryManagerPtr queryManager,
                                                         SchemaPtr& benchmarkSchema,
                                                         uint64_t ingestionRate,
                                                         uint64_t operatorId,
                                                         bool roundingNearestThousand = false) {

        auto maxTuplesPerBuffer = bufferManager->getBufferSize() / benchmarkSchema->getSchemaSizeInBytes();
        if (roundingNearestThousand) {
            NES_INFO("BM_SimpleFilterQuery: maxTuplesPerBuffer will be rounded to nearest thousands");
            maxTuplesPerBuffer = maxTuplesPerBuffer % 1000 >= 500 ? (maxTuplesPerBuffer + 1000 - maxTuplesPerBuffer % 1000)
                                                                  : (maxTuplesPerBuffer - maxTuplesPerBuffer % 1000);
        }

        NES_INFO("BM_SimpleFilterQuery: maxTuplesPerBuffer=" << maxTuplesPerBuffer);
        // at this point maxTuplesPerBuffer will be rounded to nearest thousands. This makes it easier to work with ingestion rates
        if (maxTuplesPerBuffer == 0)
            throw RuntimeException("maxTuplesPerBuffer == 0");

        return std::make_shared<SimpleBenchmarkSource>(benchmarkSchema,
                                                       bufferManager,
                                                       queryManager,
                                                       ingestionRate,
                                                       maxTuplesPerBuffer,
                                                       operatorId);
    }

  private:
    std::list<uint64_t> keyList;
    uint64_t ingestionRate;
    uint64_t keyPos = 0;
    uint64_t curNumberOfTuplesPerBuffer;
    uint64_t maxNumberOfPeriods;
    Runtime::DynamicMemoryLayout::RowLayoutPtr rowLayout;
};
}// namespace NES::Benchmarking

#endif// NES_BENCHMARK_INCLUDE_UTIL_SIMPLE_BENCHMARK_SOURCE_HPP_
