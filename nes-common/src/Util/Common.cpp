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
#include <Util/Common.hpp>
#include <Util/PocketFFT/PocketFFT.hpp>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>

namespace NES::Util {

uint64_t numberOfUniqueValues(std::vector<uint64_t>& values) {
    std::sort(values.begin(), values.end());
    return std::unique(values.begin(), values.end()) - values.begin();
}

std::string escapeJson(const std::string& str) {
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

std::string trim(std::string str) {
    auto not_space = [](char c) {
        return isspace(c) == 0;
    };
    // trim left
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), not_space));
    // trim right
    str.erase(find_if(str.rbegin(), str.rend(), not_space).base(), str.end());
    return str;
}

std::string trim(std::string str, char trimFor) {
    // remove all trimFor characters from left and right
    str.erase(std::remove(str.begin(), str.end(), trimFor), str.end());
    return str;
}

void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr) {
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

std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace) {
    if (origin.find(search) != std::string::npos) {
        return origin.replace(origin.find(search), search.size(), replace);
    }
    return origin;
}

bool endsWith(const std::string& fullString, const std::string& ending) {
    if (fullString.length() >= ending.length()) {
        // get the start of the ending index of the full string and compare with the ending string
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    }// if full string is smaller than the ending automatically return false
    return false;
}

bool startsWith(const std::string& fullString, const std::string& ending) { return (fullString.rfind(ending, 0) == 0); }

std::string toUpperCase(std::string string) {
    std::transform(string.begin(), string.end(), string.begin(), ::toupper);
    return string;
}

void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header) {
    std::ofstream ofstream(csvFileName, std::ios::trunc | std::ios::out);
    ofstream << header << std::endl;
    ofstream.close();
}

void writeRowToCsvFile(const std::string& csvFileName, const std::string& row) {
    std::ofstream ofstream(csvFileName, std::ios::app | std::ios::out);
    ofstream << row << std::endl;
    ofstream.close();
}

std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed) {
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

uint64_t murmurHash(uint64_t key) {
    uint64_t hash = key;

    hash ^= hash >> 33;
    hash *= UINT64_C(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash *= UINT64_C(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;

    return hash;
}


std::vector<std::complex<double>> fft(const std::vector<double>& lastWindowValues) {
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

std::vector<double> fftfreq(const int n, const double d) {
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

std::vector<double> psd(const std::vector<std::complex<double>>& fftValues) {
    std::vector<double> psdVec(fftValues.size());
    std::transform(fftValues.begin(), fftValues.end(), psdVec.begin(),
                   [](std::complex<double> z) { return std::norm(z); });
    return psdVec;
}

double totalEnergy(const std::vector<std::complex<double>>& fftValues) {
    double fftSum = std::accumulate(fftValues.begin(), fftValues.end(), 0.0,
                                    [](double acc, std::complex<double> z) {
                                        return acc + std::norm(z);}
    );
    return fftSum / fftValues.size();
}

std::tuple<bool, int> is_aliased_and_nyq_freq(const std::vector<double>& psd_array, const double total_energy) {
    double cutoff_percent = (total_energy * 99.0) / 100.0;
    double current_level = 0;
    uint64_t bin_idx = 0;
    while (current_level < cutoff_percent && bin_idx < psd_array.size()) {
        current_level += psd_array[bin_idx];
        ++bin_idx;
    }
    return std::make_tuple(bin_idx == psd_array.size(), bin_idx - 1);
};


std::tuple<bool, double> computeNyquistAndEnergy(const std::vector<double>& inputSignal, double intervalInSeconds) {
    // Copy the vals
    std::vector<double> currentSignal = inputSignal;
    double frequency = 1./intervalInSeconds;
    double currentNyq = 1./intervalInSeconds;

    // Calculate the mean of the elements
    double mean = std::accumulate(currentSignal.begin(), currentSignal.end(), 0.0) / inputSignal.size();
    // Detrend the mean from all elements
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

}// namespace NES::Util