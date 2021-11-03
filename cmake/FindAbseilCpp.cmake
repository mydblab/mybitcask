include(FetchContent)

FetchContent_Declare(
    abseilcpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG 20210324.2
)

FetchContent_MakeAvailable(abseilcpp)
