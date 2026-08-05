if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

cmake_minimum_required(VERSION 3.5)
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.31")
    cmake_policy(SET CMP0175 OLD)
endif()
project(arrowkdb CXX)

option(QMOD "Build module" OFF)
option(DISABLE_GCS "Disable Google Cloud Storage" OFF)

set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};${ARROW_INSTALL}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -DKXVER=3")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(MY_MODULE_NAME arrow)

if(CMAKE_SYSTEM_NAME STREQUAL Linux)
    set(A l)
elseif(CMAKE_SYSTEM_NAME STREQUAL Darwin)
    set(A m)
elseif(CMAKE_SYSTEM_NAME STREQUAL Windows)
    set(A w)
else()
    message(FATAL_ERROR "Unsupported OS")
endif()

if(APPLE)
    if(CMAKE_OSX_ARCHITECTURES MATCHES "arm64")
        set(A "${A}a64")
    elseif(CMAKE_OSX_ARCHITECTURES MATCHES "x86_64")
        set(A "${A}i64")
    else()
        message(FATAL_ERROR "Unsupported macOS architecture")
    endif()
else()
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
        set(A "${A}i64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
        set(A "${A}a64")
    else()
        message(FATAL_ERROR "Unsupported processor architecture")
    endif()
endif()

message(STATUS "A: ${A}")

set(MY_LIBRARY_NAME "clib.${A}")
file(GLOB SRC_FILES src/*.cpp)
set_source_files_properties(${SRC_FILES} PROPERTIES LANGUAGE CXX)

message(STATUS "Generator : ${CMAKE_GENERATOR}")
message(STATUS "Build Tool : ${CMAKE_BUILD_TOOL}")
message(STATUS "C API : ${ARROW_INSTALL}")

include_directories (
    ${CMAKE_BINARY_DIR} # For 'k.h', downloaded below
)

if (WIN32)
    include_directories(C:/Git/vcpkg/installed/x64-windows-static/include)
if(ARROW_INSTALL)
    include_directories("${ARROW_INSTALL}/include")
endif()

find_package(Arrow REQUIRED)
if(ARROW_INCLUDE_DIR)
    include_directories("${ARROW_INCLUDE_DIR}")
endif()
if(ARROW_SO_VERSION LESS "1000")
  set(CMAKE_CXX_STANDARD 14)
elseif(ARROW_SO_VERSION LESS "1500")
  set(CMAKE_CXX_STANDARD 17)
else()
  set(CMAKE_CXX_STANDARD 20)
endif()

find_library(ARROW_LIBRARY
    NAMES arrow
    HINTS "${ARROW_INSTALL}/lib/"
)
find_library(PARQUET_LIBRARY
    NAMES parquet
    HINTS "${ARROW_INSTALL}/lib/"
)

file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/c/c/k.h" "${CMAKE_BINARY_DIR}/k.h" )

if (MSVC)
    file(DOWNLOAD "https://github.com/KxSystems/kdb/raw/master/w64/q.lib" "${CMAKE_BINARY_DIR}/q.lib" )
    set(LINK_LIBS "${CMAKE_BINARY_DIR}/q.lib")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS") 
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W3 /D WIN32_LEAN_AND_MEAN /D _CRT_SECURE_NO_WARNINGS /D _CRT_NONSTDC_NO_DEPRECATE /D _WINSOCK_DEPRECATED_NO_WARNINGS")
    add_compile_options(/wd4710 /wd4711)
else()
    set(LINK_LIBS "")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-strict-aliasing")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-strict-aliasing")
endif()
    
add_library (${MY_LIBRARY_NAME} SHARED ${SRC_FILES})

if(LINUX)
    target_compile_options(${MY_LIBRARY_NAME} PRIVATE "-fvisibility=hidden")
endif()

IF(APPLE)
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
   set_target_properties(${MY_LIBRARY_NAME} PROPERTIES SUFFIX .so)
   set(OSFLAG m)
elseif(WIN32)
   set(OSFLAG w)
else()
   set(OSFLAG l)
endif()

target_link_libraries(${MY_LIBRARY_NAME} ${ARROW_LIBRARY}  ${PARQUET_LIBRARY} ${LINK_LIBS})
set_target_properties(${MY_LIBRARY_NAME} PROPERTIES PREFIX "")

if(DISABLE_ORC)
  target_compile_definitions(${MY_LIBRARY_NAME} PRIVATE DISABLE_ORC)
endif()

if(DISABLE_GCS)
  target_compile_definitions(${MY_LIBRARY_NAME} PRIVATE DISABLE_GCS)
endif()

if(DISABLE_AZURE)
  target_compile_definitions(${MY_LIBRARY_NAME} PRIVATE DISABLE_AZURE)
endif()

if(QMOD)
    install(TARGETS ${MY_LIBRARY_NAME} DESTINATION ${MY_MODULE_NAME})
    install(FILES q/init.q DESTINATION ${MY_MODULE_NAME})
else()
    set(MY_PRE_MOD_LIBRARY_NAME arrowkdb)
    # Check if 32-bit/64-bit machine
    set(BITNESS 32)
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(BITNESS 64)
    endif()

    # Convert QHOME env to valid cmake path
    file(TO_CMAKE_PATH $ENV{QHOME} QHOME_PATH)

    # Copy built shared object after build instead of during installation
    add_custom_command(TARGET ${MY_LIBRARY_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy "$<TARGET_FILE:${MY_LIBRARY_NAME}>" ${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/lib/${MY_PRE_MOD_LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
        DEPENDS ${MY_LIBRARY_NAME}
    )

    # Minimum build/install move binary to $QHOME/<QARCH>
    install(FILES "q/init.q" DESTINATION "${QHOME_PATH}" CONFIGURATIONS MinSizeRel Release RENAME "${MY_PRE_MOD_LIBRARY_NAME}.q")
    install(FILES "$<TARGET_FILE:${MY_LIBRARY_NAME}>" DESTINATION "${QHOME_PATH}/${OSFLAG}${BITNESS}/" CONFIGURATIONS MinSizeRel Release RENAME ${MY_PRE_MOD_LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})

    # Add the copied files to the install package
    file(COPY README.md LICENSE DESTINATION ${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/)
    file(COPY examples/ DESTINATION ${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/examples/)
    file(COPY q/ DESTINATION ${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/q/)
    file(RENAME ${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/q/init.q "${PROJECT_BINARY_DIR}/${MY_PRE_MOD_LIBRARY_NAME}/q/${MY_PRE_MOD_LIBRARY_NAME}.q")
endif()
