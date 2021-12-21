include(FetchContent)

FetchContent_Declare(   spdlog
    GIT_REPOSITORY      https://github.com/gabime/spdlog
    GIT_TAG             v1.9.2
)

FetchContent_MakeAvailable(spdlog)
