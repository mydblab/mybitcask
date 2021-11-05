# minimum required CMAKE version
cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project("mybitcask")

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(Crc32c REQUIRED)
find_package(AbseilCpp REQUIRED)
find_package(GhcFilesystem REQUIRED)

# mybitcask core library
add_library(${PROJECT_NAME}
  src/log.cc
  src/io.cc
  src/store_filename.cc
  src/store.cc
)
target_link_libraries(${PROJECT_NAME}
  crc32c
  absl::status
  absl::statusor
  absl::span
  absl::optional
  ghc_filesystem
)
target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

if (WIN32)
  target_sources(${PROJECT_NAME} PRIVATE src/io_windows.cc)
else (WIN32)
  target_sources(${PROJECT_NAME} PRIVATE src/io_posix.cc)
endif (WIN32)

# google test
find_package(Gtest)
if(Gtest_FOUND)
  enable_testing()

  include(GoogleTest)

  function(mybitcask_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)

    add_executable("${test_target_name}" "${test_file}" src/test_util.cc)
    target_link_libraries("${test_target_name}" ${PROJECT_NAME} gtest_main)

    gtest_discover_tests(${test_target_name})
  endfunction(mybitcask_test)

  mybitcask_test(src/log_test.cc)
  mybitcask_test(src/io_test.cc)
  mybitcask_test(src/store_test.cc)
  mybitcask_test(src/store_filename_test.cc)
endif()
