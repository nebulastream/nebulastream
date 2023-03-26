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
#include <Runtime/TupleBuffer.hpp>
#include <Topology/Topology.hpp>
#include <Util/Experimental/PocketFFT.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>
#include <tuple>

#include <fftw3.h>

namespace NES {

uint64_t Util::numberOfUniqueValues(std::vector<uint64_t>& values) {
    std::sort(values.begin(), values.end());
    return std::unique(values.begin(), values.end()) - values.begin();
}

std::string Util::escapeJson(const std::string& str) {
    std::ostringstream o;
    for (char c : str) {
        if (c == '"' || c == '\\' || ('\x00' <= c && c <= '\x1f')) {
            o << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int) c;
        } else {
            o << c;
        }
    }
    return o.str();
}

std::string Util::trim(std::string str) {
    auto not_space = [](char c) {
        return isspace(c) == 0;
    };
    // trim left
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), not_space));
    // trim right
    str.erase(find_if(str.rbegin(), str.rend(), not_space).base(), str.end());
    return str;
}

std::string Util::trim(std::string str, char trimFor) {
    // remove all trimFor characters from left and right
    str.erase(std::remove(str.begin(), str.end(), trimFor), str.end());
    return str;
}

std::string Util::printTupleBufferAsText(Runtime::TupleBuffer& buffer) {
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++) {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

/**
 * @brief create CSV lines from the tuples
 * @param tbuffer the tuple buffer
 * @param schema how to read the tuples from the buffer
 * @return a full string stream as string
 */
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
                NES_DEBUG2("Util::printTupleBufferAsCSV(): trying to read the variable length TEXT field: "
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
                }

                else {
                    NES_WARNING2("Util::printTupleBufferAsCSV(): Variable-length field could not be read. Invalid size in the "
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

void Util::findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
    // Get the first occurrence
    uint64_t pos = data.find(toSearch);
    // Repeat till end is reached
    while (pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos = data.find(toSearch, pos + replaceStr.size());
    }
}

std::string Util::replaceFirst(std::string origin, const std::string& search, const std::string& replace) {
    if (origin.find(search) != std::string::npos) {
        return origin.replace(origin.find(search), search.size(), replace);
    }
    return origin;
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

bool Util::endsWith(const std::string& fullString, const std::string& ending) {
    if (fullString.length() >= ending.length()) {
        // get the start of the ending index of the full string and compare with the ending string
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    }// if full string is smaller than the ending automatically return false
    return false;
}

bool Util::startsWith(const std::string& fullString, const std::string& ending) { return (fullString.rfind(ending, 0) == 0); }

std::string Util::toUpperCase(std::string string) {
    std::transform(string.begin(), string.end(), string.begin(), ::toupper);
    return string;
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
        NES_ERROR2("UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
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

std::string Util::updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed) {
    //Update the Source names by sorting and then concatenating the source names from the sub query plan
    std::vector<std::string> sourceNames;
    sourceNames.emplace_back(subQueryPlanSourceConsumed);
    sourceNames.emplace_back(queryPlanSourceConsumed);
    std::sort(sourceNames.begin(), sourceNames.end());
    // accumulating sourceNames with delimiters between all sourceNames to enable backtracking of origin
    auto updatedSourceName =
        std::accumulate(sourceNames.begin(), sourceNames.end(), std::string("-"), [](std::string a, std::string b) {
            return a + "_" + b;
        });
    return updatedSourceName;
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

}// namespace NES