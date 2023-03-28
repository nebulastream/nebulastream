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

#include <NesBaseTest.hpp>
#include <Util/Experimental/PocketFFT.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <complex>
#include <cmath>
#include <vector>

namespace NES {

template<typename T> void crand(std::vector<std::complex<T>> &v) {
    for (auto & i : v) {
        i = std::complex<T>(drand48()-0.5, drand48()-0.5);
    }
}

template<typename T> void crand(std::vector<T> &v) {
    for (auto & i : v) {
        i = drand48()-0.5;
    }
}

template<typename T1, typename T2> long double l2err (const std::vector<T1> &v1, const std::vector<T2> &v2) {
    long double sum1=0, sum2=0;
    for (size_t i=0; i<v1.size(); ++i) {
        long double dr = v1[i].real()-v2[i].real(), di = v1[i].imag()-v2[i].imag();
        long double t1 = sqrt(dr*dr+di*di), t2 = abs(v1[i]);
        sum1 += t1*t1;
        sum2 += t2*t2;
    }
    return sqrt(sum1/sum2);
}

class FFTTest : public Testing::NESBaseTest {
public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FFTTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("FFTTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("FFTTest test class TearDownTestCase."); }
};


TEST(FFTTest, pocketfftExample) {
    // we want pocketfft::c2c
    // follow https://github.com/scipy/scipy/blob/main/scipy/fft/_pocketfft/pypocketfft.cxx#L175
    for (size_t len=1; len<20; ++len) { // original test had from 1..8191 but takes time
        pocketfft::shape_t shape{len};
        pocketfft::stride_t stridef(shape.size()), strided(shape.size()), stridel(shape.size());
        size_t tmpf=sizeof(std::complex<float>),
                tmpd=sizeof(std::complex<double>),
                tmpl=sizeof(std::complex<long double>);
        for (int i=shape.size()-1; i>=0; --i) {
            stridef[i]=tmpf;
            tmpf *= shape[i];
            strided[i]=tmpd;
            tmpd *= shape[i];
            stridel[i]=tmpl;
            tmpl *= shape[i];
        }
        size_t ndata=1;
        for (size_t i=0; i<shape.size(); ++i) {
            ndata *= shape[i];
        }

        std::vector<std::complex<float>> dataf(ndata);
        std::vector<std::complex<double>> datad(ndata);
        std::vector<std::complex<long double>> datal(ndata);
        crand(dataf);
        for (size_t i=0; i<ndata; ++i) {
            datad[i] = dataf[i];
            datal[i] = dataf[i];
        }
        pocketfft::shape_t axes;
        for (size_t i=0; i<shape.size(); ++i) {
            axes.push_back(i);
        }
        auto resl = datal;
        auto resd = datad;
        auto resf = dataf;
        pocketfft::c2c(shape, stridel, stridel, axes, pocketfft::FORWARD, datal.data(), resl.data(), 1.L);
        pocketfft::c2c(shape, strided, strided, axes, pocketfft::FORWARD, datad.data(), resd.data(), 1.);
        pocketfft::c2c(shape, stridef, stridef, axes, pocketfft::FORWARD, dataf.data(), resf.data(), 1.f);
        l2err(resl, resf);
    }
    SUCCEED();
}

TEST(FFTTest, fft) {
    std::vector<double> testVector{1., 2., 3., 4.};
    std::vector<std::complex<double>> expected;
    expected.emplace_back(std::complex<double>(10, 0));
    expected.emplace_back(std::complex<double>(-2, 2));
    expected.emplace_back(std::complex<double>(-2, 0));
    expected.emplace_back(std::complex<double>(-2, -2));
    auto res = Util::fft(testVector);
    EXPECT_EQ(expected, res);
}

TEST(FFTTest, fftfreq) {
    std::vector<double> expected{0., 0.25, -0.5, -0.25};
    auto res = Util::fftfreq(4);
    EXPECT_EQ(expected, res);
}

TEST(FFTTest, fftPSD) {
    std::vector<double> expected{36., 3., 3.}; // from numpy
    std::vector<double> fftInput{1., 2., 3.};
    auto fftRes = Util::fft(fftInput);
    EXPECT_EQ(expected, Util::psd(fftRes));
}

TEST(FFTTest, fftEnergy) {
    double expected = 14.; // from numpy
    std::vector<double> fftInput{1., 2., 3.};
    auto fftRes = Util::fft(fftInput);
    EXPECT_EQ(expected, Util::totalEnergy(fftRes));
}

TEST(FFTTest, fftNyquist) {
    std::tuple<bool, double> expectedFalseBinIdx1(false, 1.);
    std::vector<double> fftInput{1., 2., 3., 4.};
    auto fftRes = Util::fft(fftInput);
    auto psd = Util::psd(fftRes);
    auto energy = Util::totalEnergy(fftRes);
    auto res = Util::is_aliased_and_nyq_freq(psd, energy);
    EXPECT_EQ(expectedFalseBinIdx1, res);
}

TEST(FFTTest, completeFftAnalysis) {
    std::vector<double> signalInput{1., 2., 3., 4., 1., -1., 0.};
    auto res = Util::computeNyquistAndEnergy(signalInput, 38.);
    EXPECT_EQ(false, std::get<0>(res));
    EXPECT_NEAR(0.046052631578947366, std::get<1>(res), .01);
}

}// namespace NES