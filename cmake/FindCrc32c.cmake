include(FetchContent)

FetchContent_Declare(
    crc32c
    GIT_REPOSITORY https://github.com/google/crc32c.git
    GIT_TAG 1.1.2
)

FetchContent_MakeAvailable(crc32c)
