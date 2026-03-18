#include <nes-sink-runtime-bindings/lib.h>

void on_error(const SinkContext& context, rust::String message)
{
    context.onError(std::string_view(message.begin(), message.end()));
}

void on_flush(const SinkContext& context, size_t epoch)
{
    context.onFlush(epoch);
}
