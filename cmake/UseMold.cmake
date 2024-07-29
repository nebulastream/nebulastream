# Linker
option(NES_USE_MOLD_IF_AVAILABLE "Uses mold for linking if it is available" ON)
find_program(MOLD mold)
if(DEFINED MOLD AND ${NES_USE_MOLD_IF_AVAILABLE})
    message(STATUS "Using mold linker")
    add_link_options("-fuse-ld=mold")
elseif (${NES_USE_MOLD_IF_AVAILABLE})
    message(STATUS "Mold is not available falling back to default linker")
endif ()

