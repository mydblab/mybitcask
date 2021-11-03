# minimum required CMAKE version
cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

project("mybitcask")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(Crc32c REQUIRED)

# mybitcask core library
add_library(${PROJECT_NAME} src/store_filename.cc src/store.cc)
target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

# google test
find_package(Gtest)
if(Gtest_FOUND)
  enable_testing()

  include(GoogleTest)

  function(mybitcask_test test_file)
    get_filename_component(test_target_name "${test_file}" NAME_WE)

    add_executable("${test_target_name}" "${test_file}")
    target_link_libraries("${test_target_name}" ${PROJECT_NAME} gtest_main)

    gtest_discover_tests(${test_target_name})
  endfunction(mybitcask_test)

  mybitcask_test(src/store_filename_test.cc)
  # mybitcask_test(src/log_codec_test.cc)
  mybitcask_test(src/store_test.cc)

endif()
