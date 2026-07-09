cmake_minimum_required(VERSION 3.20)
project(cpp_pubsub)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

option(ROBONIX_ZC_BUILD_EXAMPLES "Build cpp_pubsub example executables" OFF)

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
find_package(Threads REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(iceoryx_posh REQUIRED)
find_package(pybind11_vendor QUIET)
find_package(pybind11 QUIET)

# ── Zero-copy shared library ───────────────────────────────────────────────
# 包含所有非模板实现（ShmManager 方法 + zc_interop 函数）。
# 下游只需 link robonix_zc + include 头文件即可使用 ZcNode / ZcPublisher。
add_library(robonix_zc SHARED
  src/lib/zc_shm.cpp
  src/lib/zc_interop.cpp
)
target_include_directories(robonix_zc PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/lib>"
  "$<INSTALL_INTERFACE:include/${PROJECT_NAME}>")
ament_target_dependencies(robonix_zc rclcpp std_msgs sensor_msgs)
target_link_libraries(robonix_zc iceoryx_posh::iceoryx_posh)
target_link_libraries(robonix_zc Threads::Threads)

# 安装头文件，供下游 find_package(cpp_pubsub) 后 #include
install(DIRECTORY src/lib/
  DESTINATION include/${PROJECT_NAME}
  FILES_MATCHING PATTERN "*.hpp")

ament_export_include_directories(include/${PROJECT_NAME})
ament_export_libraries(robonix_zc)
ament_export_dependencies(rclcpp std_msgs sensor_msgs iceoryx_posh)

# Shared library — install to lib/ so ament_export_libraries can find it
install(TARGETS robonix_zc
  EXPORT export_robonix_zc
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include/${PROJECT_NAME})
ament_export_targets(export_robonix_zc HAS_LIBRARY_TARGET)

if(ROBONIX_ZC_BUILD_EXAMPLES)
  # ── 示例可执行文件 ─────────────────────────────────────────────────────────

  add_executable(talker src/publisher_example.cpp)
  ament_target_dependencies(talker rclcpp std_msgs sensor_msgs)
  target_link_libraries(talker robonix_zc)

  add_executable(listener src/subscriber_example.cpp)
  ament_target_dependencies(listener rclcpp std_msgs sensor_msgs)
  target_link_libraries(listener robonix_zc)

  add_executable(normal_talker src/test/normal_publisher_member_function.cpp)
  ament_target_dependencies(normal_talker rclcpp std_msgs sensor_msgs)

  add_executable(normal_listener src/test/normal_subscriber_member_function.cpp)
  ament_target_dependencies(normal_listener rclcpp std_msgs sensor_msgs)

  add_executable(iceoryx_image_pub src/test/iceoryx_image_publisher.cpp)
  ament_target_dependencies(iceoryx_image_pub std_msgs sensor_msgs)
  target_link_libraries(iceoryx_image_pub iceoryx_posh::iceoryx_posh)

  add_executable(iceoryx_image_sub src/test/iceoryx_image_subscriber.cpp)
  ament_target_dependencies(iceoryx_image_sub std_msgs sensor_msgs)
  target_link_libraries(iceoryx_image_sub iceoryx_posh::iceoryx_posh)

  install(TARGETS
    talker
    listener
    normal_talker
    normal_listener
    iceoryx_image_pub
    iceoryx_image_sub
    DESTINATION lib/${PROJECT_NAME})
endif()

install(PROGRAMS
  src/python/examples/py_normal_publisher_example.py
  src/python/examples/py_normal_subscriber_example.py
  src/python/examples/py_publisher_example.py
  src/python/examples/py_subscriber_example.py
  src/python/examples/py_iceoryx_direct_publisher.py
  src/python/examples/py_iceoryx_direct_subscriber.py
  DESTINATION lib/${PROJECT_NAME})

install(FILES
  src/python/examples/py_iceoryx_direct_common.py
  DESTINATION lib/${PROJECT_NAME})

if(pybind11_FOUND)
  pybind11_add_module(_zc_py_bridge src/lib/zc_py_bridge.cpp)
  target_include_directories(_zc_py_bridge PRIVATE src/lib)
  target_link_libraries(_zc_py_bridge PRIVATE
    robonix_zc
    rclcpp::rclcpp
    ${std_msgs_TARGETS}
    ${sensor_msgs_TARGETS}
    sensor_msgs::sensor_msgs_library
    iceoryx_posh::iceoryx_posh)

  set(PY_SITE_PACKAGES_DIR "lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages")
  install(
    TARGETS _zc_py_bridge
    LIBRARY DESTINATION ${PY_SITE_PACKAGES_DIR}/cpp_pubsub_zc)
  install(
    DIRECTORY src/python/cpp_pubsub_zc/
    DESTINATION ${PY_SITE_PACKAGES_DIR}/cpp_pubsub_zc)
else()
  message(WARNING "pybind11 not found, Python zero-copy bridge will not be built")
endif()
# uncomment the following section in order to fill in
# further dependencies manually.
# find_package(<dependency> REQUIRED)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # comment the line when a copyright and license is added to all source files
  set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # comment the line when a package is in a git repo and when
  # a copyright and license is added to all source files
  set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
