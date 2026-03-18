#include <emscripten/bind.h>
#include <Validator/TopologyValidator.hpp>

EMSCRIPTEN_BINDINGS(nes_validator) {
    emscripten::function("validateTopology", &NES::Validator::validateTopology);
}
