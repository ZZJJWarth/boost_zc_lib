# cpp_pubsub

`cpp_pubsub` provides the Robonix shared-memory zero-copy transport used by
the mapping stack. It builds a ROS 2/ament package that exports the
`robonix_zc` shared library, C++ headers, and optional Python bindings.

## Build

From a ROS 2 workspace:

```bash
colcon build --packages-select cpp_pubsub --cmake-args -DROBONIX_ZC_BUILD_EXAMPLES=OFF
```

Set `ROBONIX_ZC_BUILD_EXAMPLES=ON` to build the example publisher/subscriber
programs.

## Use

Downstream ROS 2 packages can depend on `cpp_pubsub`, include headers such as
`zc_shm.hpp` and `zc_pubsub.hpp`, and link against `robonix_zc`.
