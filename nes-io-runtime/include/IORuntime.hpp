#pragma once

#include <cstddef>

namespace NES
{

class IORuntime
{
public:
    static IORuntime& get();

    size_t id() const { return runtimeIdx; }

    IORuntime(const IORuntime&) = delete;
    IORuntime& operator=(const IORuntime&) = delete;

private:
    size_t runtimeIdx;

    IORuntime();
    ~IORuntime();
};

} // namespace NES
