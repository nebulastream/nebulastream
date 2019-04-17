macro (add_source PROP_NAME SOURCE_FILES)
    set(SOURCE_FILES_ABSOLUTE )
    foreach(it ${SOURCE_FILES})
        get_filename_component(ABSOLUTE_PATH ${it} ABSOLUTE)
        set(SOURCE_FILES_ABSOLUTE ${SOURCE_FILES_ABSOLUTE} ${ABSOLUTE_PATH})
    endforeach()

    get_property(OLD_PROP_VAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set_property(GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP" ${SOURCE_FILES_ABSOLUTE} ${OLD_PROP_VAL})
endmacro()

macro (get_source PROP_NAME SOURCE_FILES)
    get_property(SOURCE_FILES_LOCAL GLOBAL PROPERTY "${PROP_NAME}_SOURCE_PROP")
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro (add_source_iotdb)
    add_source(iotdb "${ARGN}")
endmacro()

macro (get_source_iotdb SOURCE_FILES)
    get_source(iotdb SOURCE_FILES_LOCAL)
    set(${SOURCE_FILES} ${SOURCE_FILES_LOCAL})
endmacro()

macro (get_header_iotdb HEADER_FILES)
    file(GLOB_RECURSE ${HEADER_FILES} "include/*.h" "include/*.hpp")
endmacro()