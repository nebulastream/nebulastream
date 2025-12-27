//
// Created by saad on 18.11.2025.
//

#include <LinuxProcessSource.hpp>

#include <iostream>

#include <cerrno>
#include <cstring>
#include <exception>
#include <utility>

#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

LinuxProcessSource::LinuxProcessSource(const SourceDescriptor& sourceDescriptor)
    : commandToRun(sourceDescriptor.getFromConfig(ConfigParametersLinuxProcess::COMMAND))
{
}

std::ostream& LinuxProcessSource::toString(std::ostream& str) const
{
    str << "\nLinuxProcessSource(";
    str << "\n  command: " << commandToRun;
    str << ")\n";
    return str;
}

void LinuxProcessSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    std::cerr << "[LinuxProcessSource] open() command=" << commandToRun << std::endl;

    NES_TRACE("LinuxProcessSource::open: Starting process with command: {}", commandToRun);
    NES_INFO("LinuxProcessSource open: {}", commandToRun);


    errno = 0;
    pipe = popen(commandToRun.c_str(), "r");
    if (!pipe) {
        char errbuf[256];
        errbuf[0] = '\0';
        (void)strerror_r(errno, errbuf, sizeof(errbuf));

        throw InvalidConfigParameter("Could not start process '{}' - {}", commandToRun, getErrorMessageFromERRNO());
    }
}

Source::FillTupleBufferResult LinuxProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    // std::cerr << "[LinuxProcessSource] fillTupleBuffer()" << std::endl;

    try
    {
        if (stopToken.stop_requested())
        {
            return FillTupleBufferResult::eos();
        }
        if (!pipe)
        {
            // consistent style: config/runtime issue -> InvalidConfigParameter is used elsewhere;
            // if you have a more specific runtime exception in the repo, use that instead.
            throw InvalidConfigParameter("LinuxProcessSource: pipe is not open (open() not called?)");
        }

        auto mem = tupleBuffer.getAvailableMemoryArea();
        if (mem.empty())
        {
            return FillTupleBufferResult::withBytes(0);
        }

        errno = 0;
        const size_t bytesRead = std::fread(mem.data(), 1, mem.size(), pipe);

        if (bytesRead > 0)
        {
            NES_DEBUG("LinuxProcessSource read {} bytes", bytesRead);
            return FillTupleBufferResult::withBytes(bytesRead);
        }

        if (std::feof(pipe))
        {
            NES_INFO("LinuxProcessSource detected EoS (process stdout EOF).");
            return FillTupleBufferResult::eos();
        }

        if (std::ferror(pipe))
        {
            NES_ERROR("LinuxProcessSource read error. errno={}", errno);
            throw std::runtime_error("LinuxProcessSource: error while reading from process");
        }

        // no bytes, no error, no eof: treat as “no progress”
        return FillTupleBufferResult::withBytes(0);
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw;
    }
}

DescriptorConfig::Config LinuxProcessSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    // same style as TCPSource
    return DescriptorConfig::validateAndFormat<ConfigParametersLinuxProcess>(std::move(config), name());
}

void LinuxProcessSource::close()
{
    // std::cerr << "[LinuxProcessSource] close()" << std::endl;

    // NES_DEBUG("LinuxProcessSource::close: Closing process.");
    if (pipe)
    {
        pclose(pipe);
        pipe = nullptr;
        // NES_TRACE("LinuxProcessSource::close: Process closed.");
    }
}

SourceValidationRegistryReturnType RegisterLinuxProcessSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return LinuxProcessSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterLinuxProcessSource(SourceRegistryArguments args)
{
    return std::make_unique<LinuxProcessSource>(args.sourceDescriptor);
}

}