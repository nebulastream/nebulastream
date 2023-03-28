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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Topology/Topology.hpp>
#include <Util/Experimental/PocketFFT.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>
#include <tuple>

namespace NES {

std::string Util::printTupleBufferAsText(Runtime::TupleBuffer& buffer) {
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

std::string Util::printTupleBufferAsCSV(Runtime::TupleBuffer tbuffer, const SchemaPtr& schema) {
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++) {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++) {
            auto field = schema->get(j);
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            // handle variable-length field
            if (dataType->isText()) {
                NES_DEBUG("Util::printTupleBufferAsCSV(): trying to read the variable length TEXT field: "
                          "from the tuple buffer");

                // read the child buffer index from the tuple buffer
                Runtime::TupleBuffer::NestedTupleBufferKey childIdx = *reinterpret_cast<uint32_t const*>(indexInBuffer);

                // retrieve the child buffer from the tuple buffer
                auto childTupleBuffer = tbuffer.loadChildBuffer(childIdx);

                // retrieve the size of the variable-length field from the child buffer
                uint32_t sizeOfTextField = *(childTupleBuffer.getBuffer<uint32_t>());

                // build the string
                if (sizeOfTextField > 0) {
                    auto begin = childTupleBuffer.getBuffer() + sizeof(uint32_t);
                    std::string deserialized(begin, begin + sizeOfTextField);
                    str = std::move(deserialized);
                } else {
                    NES_WARNING("Util::printTupleBufferAsCSV(): Variable-length field could not be read. Invalid size in the "
                                "variable-length TEXT field. Returning an empty string.")
                }
            }

            else {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str.c_str();
            if (j < schema->getSize() - 1) {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

std::string Util::toCSVString(const SchemaPtr& schema) {
    std::stringstream ss;
    for (auto& f : schema->fields) {
        ss << f->toString() << ",";
    }
    ss.seekp(-1, std::ios_base::end);
    ss << std::endl;
    return ss.str();
}

OperatorId Util::getNextOperatorId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

uint64_t Util::getNextPipelineId() {
    static std::atomic_uint64_t id = 0;
    return ++id;
}

bool Util::assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan,
                                            std::vector<std::map<std::string, std::any>> properties) {
    // count the number of operators in the query
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    size_t numOperators = queryPlanIterator.snapshot().size();
    ;

    // check if we supply operator properties for all operators
    if (numOperators != properties.size()) {
        NES_ERROR("UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
                  "operators. The query plan is: {}",
                  queryPlan->toString());
        return false;
    }

    // prepare the query plan iterator
    auto propertyIterator = properties.begin();

    // iterate over all operators in the query
    for (auto&& node : queryPlanIterator) {
        for (auto const& [key, val] : *propertyIterator) {
            // add the current property to the current operator
            node->as<LogicalOperatorNode>()->addProperty(key, val);
        }
        ++propertyIterator;
    }

    return true;
}

std::vector<Runtime::TupleBuffer> Util::createBuffersFromCSVFile(const std::string& csvFile,
                                                                 const SchemaPtr& schema,
                                                                 Runtime::BufferManagerPtr bufferManager,
                                                                 const std::string& timeStampFieldName,
                                                                 uint64_t lastTimeStamp) {
    std::vector<Runtime::TupleBuffer> recordBuffers;
    NES_ASSERT2_FMT(std::filesystem::exists(std::filesystem::path(csvFile)), "CSVFile " << csvFile << " does not exist!!!");

    // Creating everything for the csv parser
    std::ifstream file(csvFile);
    std::istream_iterator<std::string> beginIt(file);
    std::istream_iterator<std::string> endIt;
    const std::string delimiter = ",";
    auto parser = std::make_shared<CSVParser>(schema->fields.size(), getPhysicalTypes(schema), delimiter);

    // Do-while loop for checking, if we have another line to parse from the inputFile
    const auto maxTuplesPerBuffer = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();
    auto it = beginIt;
    auto tupleCount = 0UL;
    auto buffer = bufferManager->getBufferBlocking();
    do {
        std::string line = *it;
        auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        parser->writeInputTupleToTupleBuffer(line, tupleCount, dynamicTupleBuffer, schema, bufferManager);
        ++tupleCount;

        // If we have read enough tuples from the csv file, then stop iterating over it
        auto curTimeStamp = dynamicTupleBuffer[tupleCount - 1][timeStampFieldName].read<uint64_t>();
        if (curTimeStamp >= lastTimeStamp) {
            break;
        }

        if (tupleCount >= maxTuplesPerBuffer) {
            buffer.setNumberOfTuples(tupleCount);
            recordBuffers.emplace_back(buffer);
            buffer = bufferManager->getBufferBlocking();
            tupleCount = 0UL;
        }
        ++it;
    } while (it != endIt);

    if (tupleCount > 0) {
        buffer.setNumberOfTuples(tupleCount);
        recordBuffers.emplace_back(buffer);
    }

    return recordBuffers;
}

std::vector<PhysicalTypePtr> Util::getPhysicalTypes(SchemaPtr schema) {
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }
    return retVector;
}

std::string Util::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

std::vector<std::complex<double>> Util::fft(const std::vector<double>& lastWindowValues) {
    pocketfft::shape_t shape{lastWindowValues.size()};
    pocketfft::stride_t stride(1);  // always 1D shape
    pocketfft::stride_t stride_out(1);  // always 1D shape
    pocketfft::shape_t axes;
    size_t axis = 0;
    stride[0] = sizeof(double);
    stride_out[0] = 2 * sizeof(double);
    for (size_t i=0; i<shape.size(); ++i) {
        axes.push_back(i);
    }
    std::vector<std::complex<double>> r2cRes(lastWindowValues.size());
    pocketfft::r2c(shape, stride, stride_out, axis, pocketfft::FORWARD, lastWindowValues.data(), r2cRes.data(), 1.);
    pocketfft::detail::ndarr<std::complex<double>> ares(r2cRes.data(), shape, stride_out);
    pocketfft::detail::rev_iter iter(ares, axes);
    while(iter.remaining()>0) {
        auto v = ares[iter.ofs()];
        ares[iter.rev_ofs()] = conj(v);
        iter.advance();
    }
    return r2cRes;
}

std::vector<double> Util::fftfreq(const int n, const double d) {
    double val = 1.0 / (n * d);
    std::vector<double> results(n);
    int N = (n - 1) / 2 + 1;
    std::vector<int> p1(N);
    std::iota(p1.begin(), p1.end(), 0);
    std::vector<int> p2(n - N);
    std::iota(p2.begin(), p2.end(), -(n / 2));
    std::copy(p1.begin(), p1.end(), results.begin());
    std::copy(p2.begin(), p2.end(), results.begin() + N);
    std::transform(results.begin(), results.end(), results.begin(), [val](double x) {
        return x * val;
    });
    return results;
}

std::vector<double> Util::psd(const std::vector<std::complex<double>>& fftValues) {
    std::vector<double> psdVec(fftValues.size());
    std::transform(fftValues.begin(), fftValues.end(), psdVec.begin(),
                   [](std::complex<double> z) { return std::norm(z); });
    return psdVec;
}

double Util::totalEnergy(const std::vector<std::complex<double>>& fftValues) {
    double fftSum = std::accumulate(fftValues.begin(), fftValues.end(), 0.0,
                                    [](double acc, std::complex<double> z) {
                                        return acc + std::norm(z);}
                                    );
    return fftSum / fftValues.size();
}

std::tuple<bool, int> Util::is_aliased_and_nyq_freq(const std::vector<double>& psd_array, const double total_energy) {
    double cutoff_percent = (total_energy * 99.0) / 100.0;
    double current_level = 0;
    uint64_t bin_idx = 0;
    while (current_level < cutoff_percent && bin_idx < psd_array.size()) {
        current_level += psd_array[bin_idx];
        bin_idx++;
    }
    return std::make_tuple(bin_idx == psd_array.size(), bin_idx);
};


std::tuple<bool, double> Util::computeNyquistAndEnergy(const std::vector<double>& inputSignal, double interval) {
    // Copy the vals
    std::vector<double> currentSignal = inputSignal;
    double frequency = 1./interval;
    double currentNyq = 1./interval;

    // Calculate the mean of non-NaN elements
    auto mean_it = std::find_if(currentSignal.begin(), currentSignal.end(), [](double x){ return !std::isnan(x); });
    double mean = std::accumulate(mean_it, currentSignal.end(), 0.0) / std::distance(mean_it, currentSignal.end());
    // Subtract the mean from each element
    std::transform(currentSignal.begin(), currentSignal.end(), currentSignal.begin(), [mean](double x){ return x - mean; });

    // Perform 1D FFT on the detrended values
    auto sensorFft = Util::fft(currentSignal);
    // Compute corresponding frequencies
    auto sensorFftFreq = Util::fftfreq(inputSignal.size(), frequency);
    // Get the psd of the FFT
    auto psd = Util::psd(sensorFft);
    // Get energy of incoming signal
    auto E_fft = Util::totalEnergy(sensorFft);
    // Get isAliased, proposed new rated idx in psd
    auto aliasingResult = Util::is_aliased_and_nyq_freq(psd, E_fft);
    auto isAliased = std::get<0>(aliasingResult);
    auto nyqIdx = std::get<1>(aliasingResult);
    if (!isAliased && E_fft > 0.) {
        // Use 2x the proposed nyquist rate
        currentNyq = 2 * sensorFftFreq[nyqIdx];
    }

    return std::make_tuple(currentNyq < frequency, 1./currentNyq);
};

}// namespace NES