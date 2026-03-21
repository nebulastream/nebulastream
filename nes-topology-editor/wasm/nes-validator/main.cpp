#include <emscripten/bind.h>
#include <Validator/TopologyValidator.hpp>
#include <Validator/DumpPipeline.hpp>
#include <Validator/ConfigMetadata.hpp>

EMSCRIPTEN_BINDINGS(nes_validator) {
    emscripten::function("validateTopology", &NES::Validator::validateTopology);
    emscripten::function("explainTopology", &NES::Validator::explainTopology);
    emscripten::function("getRegisteredSourceTypes", &NES::Validator::getRegisteredSourceTypes);
    emscripten::function("getRegisteredSinkTypes", &NES::Validator::getRegisteredSinkTypes);
    emscripten::function("getSourceConfigFields", &NES::Validator::getSourceConfigFields);
    emscripten::function("getSinkConfigFields", &NES::Validator::getSinkConfigFields);
}
