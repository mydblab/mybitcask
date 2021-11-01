# minimum required CMAKE version
cmake_minimum_required(VERSION 3.11 FATAL_ERROR)

project("mybitcask")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

find_package(Crc32c REQUIRED)
find_package(Gtest)
if(Gtest_FOUND)
enable_testing()

add_executable(
  codec_test
  codec_test.cc
)
target_link_libraries(
  codec_test
  gtest_main
)

include(GoogleTest)
gtest_discover_tests(codec_test)
endif()


# mybitcask core library
add_library(${PROJECT_NAME})
target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)
