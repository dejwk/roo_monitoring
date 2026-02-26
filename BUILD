load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

cc_library(
    name = "roo_monitoring",
    srcs = glob(
        [
            "src/**/*.cpp",
            "src/**/*.h",
        ],
    ),
    includes = [
        "src",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@roo_collections",
        "@roo_io",
        "@roo_logging",
        "@roo_testing//roo_testing:arduino",
    ],
)

cc_test(
    name = "monitoring_test",
    size = "small",
    srcs = [
        "test/monitoring_test.cpp",
    ],
    includes = [
        "src",
    ],
    linkstatic = 1,
    deps = [
        ":roo_monitoring",
        "@roo_io//test/fs:fakefs",
        "@roo_testing//:arduino_gtest_main",
    ],
)
