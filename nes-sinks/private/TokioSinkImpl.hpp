#pragma once

#include <Sinks/TokioSink.hpp>
#include <nes-sink-runtime-bindings/lib.h>

struct NES::TokioSink::RustSinkHandleImpl
{
    rust::Box<::SinkHandle> handle;
    explicit RustSinkHandleImpl(rust::Box<::SinkHandle> h) : handle(std::move(h)) {}
};
