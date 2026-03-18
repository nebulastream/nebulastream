#include <emscripten/bind.h>
#include <string>

std::string greet(const std::string& name) {
    return "Hello from WASM, " + name + "!";
}

EMSCRIPTEN_BINDINGS(spike_embind) {
    emscripten::function("greet", &greet);
}
