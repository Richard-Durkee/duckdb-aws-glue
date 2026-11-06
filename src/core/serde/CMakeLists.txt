add_library_unity(glue_core_serde OBJECT hive_csv_dialect.cpp hive_json_dialect.cpp
                  hive_serde.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core_serde>
    PARENT_SCOPE)
