# Licensed under the Apache License, Version 2.0 (the "License").
include(CMakeParseArguments)

function(add_codon_extension_plugin TARGET)
    cmake_parse_arguments(ARG "" "PLUGIN_NAME;CODON_PATH" "ADDITIONAL_SOURCES;NATIVE_SYMBOLS;NATIVE_SYMBOL_HEADERS;LINK_LIBRARIES" ${ARGN})
    if (NOT ARG_PLUGIN_NAME)
        set(ARG_PLUGIN_NAME "${TARGET}")
    endif ()
    if (ARG_CODON_PATH)
        if (NOT IS_DIRECTORY "${ARG_CODON_PATH}")
            message(FATAL_ERROR "Codon plugin ${TARGET} CODON_PATH is not a directory: ${ARG_CODON_PATH}")
        endif ()
        file(GLOB_RECURSE _codon_resources CONFIGURE_DEPENDS RELATIVE "${ARG_CODON_PATH}"
            "${ARG_CODON_PATH}/*.codon"
            "${ARG_CODON_PATH}/*.py")
    endif ()
    if (NOT _codon_resources AND NOT ARG_NATIVE_SYMBOLS)
        message(FATAL_ERROR "Codon plugin ${TARGET} must provide modules, native symbols, or both")
    endif ()

    set(NES_CODON_PLUGIN_MODULE_DECLARATIONS "")
    set(NES_CODON_PLUGIN_MODULE_ENTRIES "")
    set(_module_index 0)
    foreach (_module_path IN LISTS _codon_resources)
        file(READ "${ARG_CODON_PATH}/${_module_path}" _module_source)
        string(APPEND NES_CODON_PLUGIN_MODULE_DECLARATIONS
            "static constexpr char module_${_module_index}[] = R\"NES_CODON_MODULE(${_module_source})NES_CODON_MODULE\";\n")
        string(APPEND NES_CODON_PLUGIN_MODULE_ENTRIES
            "    {\"${_module_path}\", module_${_module_index}, sizeof(module_${_module_index}) - 1},\n")
        math(EXPR _module_index "${_module_index} + 1")
    endforeach ()

    set(NES_CODON_PLUGIN_NATIVE_SYMBOL_INCLUDES "")
    foreach (_header IN LISTS ARG_NATIVE_SYMBOL_HEADERS)
        string(APPEND NES_CODON_PLUGIN_NATIVE_SYMBOL_INCLUDES "#include <${_header}>\n")
    endforeach ()
    set(NES_CODON_PLUGIN_NATIVE_SYMBOL_ENTRIES "")
    foreach (_symbol IN LISTS ARG_NATIVE_SYMBOLS)
        string(APPEND NES_CODON_PLUGIN_NATIVE_SYMBOL_ENTRIES
            "    {\"${_symbol}\", reinterpret_cast<void*>(&${_symbol})},\n")
    endforeach ()
    set(NES_CODON_PLUGIN_NAME "${ARG_PLUGIN_NAME}")
    set(NES_CODON_PLUGIN_MODULE_COUNT "${_module_index}")
    list(LENGTH ARG_NATIVE_SYMBOLS NES_CODON_PLUGIN_NATIVE_SYMBOL_COUNT)
    if (_module_index EQUAL 0)
        set(NES_CODON_PLUGIN_MODULE_STORAGE "static constexpr const NESCodonModuleV1* modules = nullptr;")
    else ()
        set(NES_CODON_PLUGIN_MODULE_STORAGE
            "static constexpr NESCodonModuleV1 modules[] = {\n${NES_CODON_PLUGIN_MODULE_ENTRIES}};")
    endif ()
    if (NES_CODON_PLUGIN_NATIVE_SYMBOL_COUNT EQUAL 0)
        set(NES_CODON_PLUGIN_NATIVE_SYMBOL_STORAGE "static constexpr const NESCodonNativeSymbolV1* nativeSymbols = nullptr;")
    else ()
        set(NES_CODON_PLUGIN_NATIVE_SYMBOL_STORAGE
            "static const NESCodonNativeSymbolV1 nativeSymbols[] = {\n${NES_CODON_PLUGIN_NATIVE_SYMBOL_ENTRIES}};")
    endif ()

    set(_generated_source "${CMAKE_CURRENT_BINARY_DIR}/${TARGET}Descriptor.cpp")
    configure_file("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/NESCodonPluginDescriptor.cpp.in" "${_generated_source}" @ONLY)

    add_library(${TARGET} MODULE ${ARG_ADDITIONAL_SOURCES} "${_generated_source}")
    set_target_properties(${TARGET} PROPERTIES PREFIX "" CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES)
    if (NOT NES_CODON_PLUGIN_INCLUDE_DIR)
        set(NES_CODON_PLUGIN_INCLUDE_DIR "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../include")
    endif ()
    target_include_directories(${TARGET} PRIVATE "${NES_CODON_PLUGIN_INCLUDE_DIR}")
    target_link_libraries(${TARGET} PRIVATE ${ARG_LINK_LIBRARIES})
    if (UNIX AND NOT APPLE)
        # Codon extensions import the seq_* host ABI. Those symbols are resolved from the
        # NES executable when dlopen loads the module.
        target_link_options(${TARGET} PRIVATE "LINKER:-z,undefs")
        target_link_options(${TARGET} PRIVATE "LINKER:--exclude-libs,ALL")
    endif ()
endfunction()
