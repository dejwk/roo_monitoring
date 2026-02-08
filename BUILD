load("@rules_cc//cc:cc_library.bzl", "cc_library")

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
