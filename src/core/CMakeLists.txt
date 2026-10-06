add_library_unity(glue_core OBJECT glue_info.cpp glue_types.cpp
                  hive_compression.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core>
    PARENT_SCOPE)
