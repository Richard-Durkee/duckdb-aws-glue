add_library_unity(
  glue_core_serde
  OBJECT
  hive_csv_dialect.cpp
  hive_json_dialect.cpp
  hive_serde.cpp
  hcatalog_json_serde.cpp
  lazy_simple_serde.cpp
  open_csv_serde.cpp
  openx_json_serde.cpp)
set(GLUE_EXTENSION_FILES
    ${GLUE_EXTENSION_FILES} $<TARGET_OBJECTS:glue_core_serde>
    PARENT_SCOPE)
