include(FetchContent)

FetchContent_Declare(
    ghcfilesystem
    GIT_REPOSITORY https://github.com/gulrak/filesystem.git
    GIT_TAG v1.5.10
)

FetchContent_MakeAvailable(ghcfilesystem)
