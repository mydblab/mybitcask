include(FetchContent)

FetchContent_Declare(
    crc32c
    GIT_REPOSITORY https://github.com/google/crc32c.git
    GIT_TAG 1.1.2
)

option(CRC32C_BUILD_TESTS  OFF)
option(CRC32C_BUILD_BENCHMARKS  OFF)
option(CRC32C_USE_GLOG  OFF)

FetchContent_MakeAvailable(crc32c)
