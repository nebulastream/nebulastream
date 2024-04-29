macro(project_enable_fixguards)
    include(FetchContent)
    FetchContent_Declare(guardonce
            GIT_REPOSITORY https://github.com/cgmb/guardonce.git
            GIT_TAG        a830d4638723f9e619a1462f470fb24df1cb08dd
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
    )
    FetchContent_MakeAvailable(guardonce)

    add_custom_target(fix-guards
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-benchmark/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-benchmark/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-catalogs/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-catalogs/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-client/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-client/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r -e="*version.hpp" -e="*backward.hpp" -e="*base64.h" -e="*libcuckoo/*" -e="*magicenum/*" nes-common/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -e="*version.hpp" -e="*backward.hpp" -e="*base64.h" -e="*libcuckoo/*" -e="*magicenum/*" -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-common/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-common/tests/Util/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-common/tests/Util/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-compiler/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-compiler/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-configurations/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-configurations/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-coordinator/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-coordinator/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-coordinator/tests/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-coordinator/tests/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-data-types/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-data-types/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r -e="*digestible.h" -e="*HyperLogLog.hpp" -e="*Yaml.hpp" nes-execution/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -e="*digestible.h" -e="*HyperLogLog.hpp" -e="*Yaml.hpp" -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-execution/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-execution/tests/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-execution/tests/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-expressions/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-expressions/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-nautilus/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-nautilus/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-operators/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-operators/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-operators/tests/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-operators/tests/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-optimizer/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-optimizer/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-plugins/arrow/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-plugins/arrow/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-plugins/onnx/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-plugins/onnx/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-plugins/tensorflow/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-plugins/tensorflow/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r -e="*apex_memmove.hpp" -e="*jitify.hpp" -e="*JNI.hpp" -e="*rte_memory.h" nes-runtime/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -e="*apex_memmove.hpp" -e="*jitify.hpp" -e="*JNI.hpp" -e="*rte_memory.h" -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-runtime/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-statistics/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-statistics/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-window-types/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-window-types/include/

        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.guard2once -r nes-worker/include/
        COMMAND PYTHONPATH=${guardonce_SOURCE_DIR} python3 -m guardonce.once2guard -r -p 'path | append _ | upper' -s '\#endif  // %\\n' nes-worker/include/

        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
    message(STATUS "guardonce utility to fix include guards is available via the 'fix-guards' target")
endmacro(project_enable_fixguards)