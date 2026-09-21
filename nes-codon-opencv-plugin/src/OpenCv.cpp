// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include "OpenCv.hpp"

extern "C" void* seq_alloc(size_t size);

namespace NES
{
namespace
{
thread_local std::array<int8_t, 4096> openCvError;
thread_local std::vector<uint8_t> encodedImage;

void setOpenCvError(const std::string_view message, int8_t** errorPointer, uint64_t* errorSize) noexcept
{
    const auto copiedSize = std::min(message.size(), openCvError.size());
    std::memcpy(openCvError.data(), message.data(), copiedSize);
    *errorPointer = openCvError.data();
    *errorSize = copiedSize;
}

template <typename Function>
uint8_t invokeOpenCv(Function&& function, int8_t** errorPointer, uint64_t* errorSize) noexcept
{
    uint8_t status = 1;
    *errorPointer = nullptr;
    *errorSize = 0;
    try
    {
        function();
        status = 0;
    }
    catch (const std::exception& exception)
    {
        setOpenCvError(exception.what(), errorPointer, errorSize);
    }
    catch (...)
    {
        setOpenCvError("OpenCV failed with an unknown exception", errorPointer, errorSize);
    }
    return status;
}

cv::Mat wrapImage(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth)
{
    if (width == 0 || height == 0 || channels == 0 || channels > CV_CN_MAX || depth > CV_16F)
    {
        throw std::invalid_argument("NES cv2 received invalid image dimensions, channel count, or element type");
    }
    if (width > std::numeric_limits<int>::max() || height > std::numeric_limits<int>::max())
    {
        throw std::invalid_argument("NES cv2 image dimensions exceed OpenCV limits");
    }
    const auto elementSize = CV_ELEM_SIZE1(CV_MAKETYPE(static_cast<int>(depth), 1));
    if (width > std::numeric_limits<uint64_t>::max() / height || width * height > std::numeric_limits<uint64_t>::max() / channels
        || width * height * channels > std::numeric_limits<uint64_t>::max() / elementSize
        || imageSize != width * height * channels * elementSize)
    {
        throw std::invalid_argument("NES cv2 received an invalid contiguous image size");
    }
    return cv::Mat(
        static_cast<int>(height),
        static_cast<int>(width),
        CV_MAKETYPE(static_cast<int>(depth), static_cast<int>(channels)),
        const_cast<uint8_t*>(imagePointer));
}

cv::Mat
wrapUint8Image(const uint8_t* imagePointer, const uint64_t imageSize, const uint64_t width, const uint64_t height, const uint64_t channels)
{
    return wrapImage(imagePointer, imageSize, width, height, channels, CV_8U);
}

int checkedOpenCvEnum(const uint64_t value, const std::string_view parameter)
{
    if (value > static_cast<uint64_t>(std::numeric_limits<int>::max()))
    {
        throw std::invalid_argument("NES cv2 received an out-of-range " + std::string{parameter});
    }
    return static_cast<int>(value);
}

void exportMat(
    const cv::Mat& image,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize)
{
    if (image.empty())
    {
        throw std::runtime_error("OpenCV produced an empty image");
    }
    if (!image.isContinuous())
    {
        throw std::runtime_error("OpenCV produced a non-contiguous image");
    }
    const auto size = image.total() * image.elemSize();
    auto* destination = static_cast<int8_t*>(seq_alloc(size));
    std::memcpy(destination, image.data, size);
    *resultPointer = destination;
    *resultSize = size;
    *resultWidth = image.cols;
    *resultHeight = image.rows;
    *resultChannels = image.channels();
    *resultElementSize = image.elemSize1();
}

void initializeImageResult(
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize)
{
    *resultPointer = nullptr;
    *resultSize = 0;
    *resultWidth = 0;
    *resultHeight = 0;
    *resultChannels = 0;
    *resultElementSize = 0;
}

}

extern "C" uint8_t nes_cv_imencode(
    const int8_t* extensionPointer,
    const uint64_t extensionSize,
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const int64_t* parameterPointer,
    const uint64_t parameterCount,
    int8_t** resultPointer,
    uint64_t* resultSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    uint8_t status = 1;
    *resultPointer = nullptr;
    *resultSize = 0;
    *errorPointer = nullptr;
    *errorSize = 0;

    try
    {
        const std::string_view extension{reinterpret_cast<const char*>(extensionPointer), extensionSize};
        const auto image = wrapImage(imagePointer, imageSize, width, height, channels, depth);
        std::vector<int> parameters;
        parameters.reserve(parameterCount);
        for (uint64_t index = 0; index < parameterCount; ++index)
        {
            if (parameterPointer[index] < std::numeric_limits<int>::min() || parameterPointer[index] > std::numeric_limits<int>::max())
            {
                throw std::invalid_argument("NES cv2.imencode received an out-of-range encoder parameter");
            }
            parameters.push_back(static_cast<int>(parameterPointer[index]));
        }
        encodedImage.clear();
        if (!cv::imencode(std::string{extension}, image, encodedImage, parameters))
        {
            throw std::runtime_error("OpenCV could not encode the image");
        }

        auto* destination = static_cast<int8_t*>(seq_alloc(encodedImage.size()));
        std::memcpy(destination, encodedImage.data(), encodedImage.size());
        *resultPointer = destination;
        *resultSize = encodedImage.size();
        status = 0;
    }
    catch (const std::exception& exception)
    {
        setOpenCvError(exception.what(), errorPointer, errorSize);
    }
    catch (...)
    {
        setOpenCvError("OpenCV failed with an unknown exception", errorPointer, errorSize);
    }
    return status;
}

extern "C" uint8_t nes_cv_cvt_color(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const uint64_t conversion,
    const uint64_t destinationChannels,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            const auto input = wrapImage(imagePointer, imageSize, width, height, channels, depth);
            cv::Mat output;
            cv::cvtColor(
                input,
                output,
                checkedOpenCvEnum(conversion, "color conversion code"),
                checkedOpenCvEnum(destinationChannels, "destination channel count"));
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_apply_color_map(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t colorMap,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            const auto input = wrapUint8Image(imagePointer, imageSize, width, height, channels);
            cv::Mat output;
            cv::applyColorMap(input, output, checkedOpenCvEnum(colorMap, "color map"));
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_resize(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const uint64_t targetWidth,
    const uint64_t targetHeight,
    const double scaleX,
    const double scaleY,
    const uint64_t interpolation,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            if (targetWidth > std::numeric_limits<int>::max() || targetHeight > std::numeric_limits<int>::max())
            {
                throw std::invalid_argument("NES cv2.resize target dimensions exceed OpenCV limits");
            }
            const auto input = wrapImage(imagePointer, imageSize, width, height, channels, depth);
            cv::Mat output;
            cv::resize(
                input,
                output,
                cv::Size{static_cast<int>(targetWidth), static_cast<int>(targetHeight)},
                scaleX,
                scaleY,
                checkedOpenCvEnum(interpolation, "interpolation mode"));
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_imdecode(
    const uint8_t* inputPointer,
    const uint64_t inputSize,
    const int64_t flags,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            if (inputSize == 0 || inputSize > static_cast<uint64_t>(std::numeric_limits<int>::max()))
            {
                throw std::invalid_argument("NES cv2.imdecode received an invalid encoded image size");
            }
            if (flags < std::numeric_limits<int>::min() || flags > std::numeric_limits<int>::max())
            {
                throw std::invalid_argument("NES cv2 received out-of-range image decode flags");
            }
            const cv::Mat encoded(1, static_cast<int>(inputSize), CV_8UC1, const_cast<uint8_t*>(inputPointer));
            const auto output = cv::imdecode(encoded, static_cast<int>(flags));
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_warp_affine(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const double matrix00,
    const double matrix01,
    const double matrix02,
    const double matrix10,
    const double matrix11,
    const double matrix12,
    const uint64_t targetWidth,
    const uint64_t targetHeight,
    const uint64_t flags,
    const uint64_t borderMode,
    const double borderValue0,
    const double borderValue1,
    const double borderValue2,
    const double borderValue3,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            if (targetWidth == 0 || targetHeight == 0 || targetWidth > std::numeric_limits<int>::max()
                || targetHeight > std::numeric_limits<int>::max())
            {
                throw std::invalid_argument("NES cv2.warpAffine received invalid target dimensions");
            }
            const auto input = wrapImage(imagePointer, imageSize, width, height, channels, depth);
            const cv::Matx23d transformation{matrix00, matrix01, matrix02, matrix10, matrix11, matrix12};
            cv::Mat output;
            cv::warpAffine(
                input,
                output,
                transformation,
                cv::Size{static_cast<int>(targetWidth), static_cast<int>(targetHeight)},
                checkedOpenCvEnum(flags, "warp flags"),
                checkedOpenCvEnum(borderMode, "border mode"),
                cv::Scalar{borderValue0, borderValue1, borderValue2, borderValue3});
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_normalize(
    const uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const double alpha,
    const double beta,
    const uint64_t normType,
    const int64_t destinationDepth,
    int8_t** resultPointer,
    uint64_t* resultSize,
    uint64_t* resultWidth,
    uint64_t* resultHeight,
    uint64_t* resultChannels,
    uint64_t* resultElementSize,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    initializeImageResult(resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
    return invokeOpenCv(
        [&]
        {
            if (destinationDepth < -1 || destinationDepth > CV_16F)
            {
                throw std::invalid_argument("NES cv2.normalize received an invalid destination depth");
            }
            const auto input = wrapImage(imagePointer, imageSize, width, height, channels, depth);
            cv::Mat output;
            cv::normalize(
                input, output, alpha, beta, checkedOpenCvEnum(normType, "normalization type"), static_cast<int>(destinationDepth));
            exportMat(output, resultPointer, resultSize, resultWidth, resultHeight, resultChannels, resultElementSize);
        },
        errorPointer,
        errorSize);
}

extern "C" uint8_t nes_cv_rectangle(
    uint8_t* imagePointer,
    const uint64_t imageSize,
    const uint64_t width,
    const uint64_t height,
    const uint64_t channels,
    const uint64_t depth,
    const int64_t x1,
    const int64_t y1,
    const int64_t x2,
    const int64_t y2,
    const double color0,
    const double color1,
    const double color2,
    const int64_t thickness,
    const int64_t lineType,
    const int64_t shift,
    int8_t** errorPointer,
    uint64_t* errorSize)
{
    return invokeOpenCv(
        [&]
        {
            auto image = wrapImage(imagePointer, imageSize, width, height, channels, depth);
            cv::rectangle(
                image,
                cv::Point{static_cast<int>(x1), static_cast<int>(y1)},
                cv::Point{static_cast<int>(x2), static_cast<int>(y2)},
                cv::Scalar{color0, color1, color2},
                static_cast<int>(thickness),
                static_cast<int>(lineType),
                static_cast<int>(shift));
        },
        errorPointer,
        errorSize);
}

}
