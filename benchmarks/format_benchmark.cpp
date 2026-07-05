#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

static uint64_t countExactOutputSize(const std::string& fmt, const std::vector<std::string>& args)
{
    uint64_t size = 0;
    uint64_t argIdx = 0;

    for (uint64_t i = 0; i < fmt.size(); ++i)
    {
        if (fmt[i] == '{' && i + 1 < fmt.size() && fmt[i + 1] == '}')
        {
            size += args[argIdx++].size();
            ++i;
        }
        else if (fmt[i] == '{' && i + 1 < fmt.size() && fmt[i + 1] == '{')
        {
            ++size;
            ++i;
        }
        else if (fmt[i] == '}' && i + 1 < fmt.size() && fmt[i + 1] == '}')
        {
            ++size;
            ++i;
        }
        else
        {
            ++size;
        }
    }

    return size;
}

static uint64_t worstCaseSize(const std::string& fmt, const std::vector<std::string>& args)
{
    uint64_t size = fmt.size();

    for (const auto& arg : args)
    {
        size += arg.size();
    }

    return size;
}

static uint64_t writeFormattedOutput(const std::string& fmt, const std::vector<std::string>& args, char* out)
{
    uint64_t outIdx = 0;
    uint64_t argIdx = 0;

    for (uint64_t i = 0; i < fmt.size(); ++i)
    {
        if (fmt[i] == '{')
        {
            if (i + 1 < fmt.size() && fmt[i + 1] == '}')
            {
                const auto& arg = args[argIdx++];
                std::memcpy(out + outIdx, arg.data(), arg.size());
                outIdx += arg.size();
                ++i;
            }
            else if (i + 1 < fmt.size() && fmt[i + 1] == '{')
            {
                out[outIdx++] = '{';
                ++i;
            }
            else
            {
                out[outIdx++] = fmt[i];
            }
        }
        else if (fmt[i] == '}')
        {
            if (i + 1 < fmt.size() && fmt[i + 1] == '}')
            {
                out[outIdx++] = '}';
                ++i;
            }
            else
            {
                out[outIdx++] = fmt[i];
            }
        }
        else
        {
            out[outIdx++] = fmt[i];
        }
    }

    return outIdx;
}

static uint64_t strategyExactTwoPass(
    const std::string& fmt,
    const std::vector<std::string>& args,
    std::string& output)
{
    const auto exactSize = countExactOutputSize(fmt, args);
    output.resize(exactSize);
    return writeFormattedOutput(fmt, args, output.data());
}

static uint64_t strategyWorstCasePerRow(
    const std::string& fmt,
    const std::vector<std::string>& args,
    std::string& output)
{
    const auto maxSize = worstCaseSize(fmt, args);
    output.resize(maxSize);

    const auto actualSize = writeFormattedOutput(fmt, args, output.data());
    output.resize(actualSize);

    return actualSize;
}

static uint64_t strategyWorstCasePreallocated(
    const std::string& fmt,
    const std::vector<std::string>& args,
    char* preallocBuf)
{
    return writeFormattedOutput(fmt, args, preallocBuf);
}

static void runBenchmark(
    const std::string& name,
    const std::string& fmt,
    const std::vector<std::string>& args,
    uint64_t rows)
{
    std::cout << "\n=== " << name << " ===\n";

    const uint64_t worstCase = worstCaseSize(fmt, args);

    {
        std::string output;
        uint64_t checksum = 0;

        const auto start = Clock::now();

        for (uint64_t i = 0; i < rows; ++i)
        {
            checksum += strategyExactTwoPass(fmt, args, output);
        }

        const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();

        std::cout << "A | exact-size two-pass"
                  << " | time=" << seconds << "s"
                  << " | rows/s=" << static_cast<double>(rows) / seconds
                  << " | checksum=" << checksum
                  << '\n';
    }

    {
        std::string output;
        uint64_t checksum = 0;

        const auto start = Clock::now();

        for (uint64_t i = 0; i < rows; ++i)
        {
            checksum += strategyWorstCasePerRow(fmt, args, output);
        }

        const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();

        std::cout << "B | Upper-bound one-pass allocation"
                  << " | time=" << seconds << "s"
                  << " | rows/s=" << static_cast<double>(rows) / seconds
                  << " | checksum=" << checksum
                  << " | worst_case_size=" << worstCase
                  << '\n';
    }

    {
        std::vector<char> preallocBuffer(worstCase);
        uint64_t checksum = 0;

        const auto start = Clock::now();

        for (uint64_t i = 0; i < rows; ++i)
        {
            checksum += strategyWorstCasePreallocated(fmt, args, preallocBuffer.data());
        }

        const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();

        std::cout << "C | reusable worst-case buffer"
                  << " | time=" << seconds << "s"
                  << " | rows/s=" << static_cast<double>(rows) / seconds
                  << " | checksum=" << checksum
                  << " | buffer_size=" << worstCase
                  << '\n';
    }
}

int main()
{
    constexpr uint64_t rows = 100'000'000;

    runBenchmark(
        "one_arg",
        "value={}",
        {"123456"},
        rows);

    runBenchmark(
        "three_args",
        "a={}, b={}, c={}",
        {"123", "456", "789"},
        rows);

    runBenchmark(
        "seven_args",
        "{}-{}-{}-{}-{}-{}-{}",
        {"1", "2", "3", "4", "5", "6", "7"},
        rows);

    runBenchmark(
        "escaped_braces",
        "{{ value={} }}",
        {"123456"},
        rows);

    runBenchmark(
        "long_literal",
        "prefix-prefix-prefix-prefix value={} suffix-suffix-suffix-suffix",
        {"123456789"},
        rows);

    return 0;
}