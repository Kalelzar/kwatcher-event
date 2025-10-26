const std = @import("std");

pub fn build(b: *std.Build) !void {
    // Options
    const build_all = b.option(bool, "all", "Build all components. You can still disable individual components") orelse false;
    const build_static_library = b.option(bool, "lib", "Build a static library object") orelse build_all;
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const kwatcher_event_library = b.addModule("kwatcher_event", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const tests = b.addTest(.{
        .root_module = kwatcher_event_library,
    });

    // Artifacts:
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "lib-kwatcher-event",
        .root_module = kwatcher_event_library,
    });
    if (build_static_library) {
        b.installArtifact(lib);
    }

    const run_tests = b.addRunArtifact(tests);

    const install_docs = b.addInstallDirectory(
        .{
            .source_dir = lib.getEmittedDocs(),
            .install_dir = .prefix,
            .install_subdir = "docs",
        },
    );

    const fmt = b.addFmt(.{
        .paths = &.{
            "src/",
            "build.zig",
            "build.zig.zon",
        },
        .check = true,
    });

    // Steps:
    const check = b.step("check", "Build without generating artifacts.");
    check.dependOn(&lib.step);

    const test_step = b.step("test", "Run the unit tests.");
    test_step.dependOn(&run_tests.step);
    // - fmt
    const fmt_step = b.step("fmt", "Check formatting");
    fmt_step.dependOn(&fmt.step);
    check.dependOn(fmt_step);
    b.getInstallStep().dependOn(fmt_step);
    // - docs
    const docs_step = b.step("docs", "Generate docs");
    docs_step.dependOn(&install_docs.step);
    docs_step.dependOn(&lib.step);

    // Dependencies:
    // 1st Party:
    const kw = b.dependency("kwatcher", .{
        .target = target,
        .optimize = optimize,
        .lib = true,
        .example = false,
        .dump = false,
    });
    const kwatcher = kw.module("kwatcher");
    // 3rd Party:
    const pg = b.dependency("pg", .{ .target = target, .optimize = optimize }).module("pg");
    const uuid = kw.builder.dependency("uuid", .{ .target = target, .optimize = optimize }).module("uuid");
    // Imports:
    // Internal:
    // 1st Party:
    kwatcher_event_library.addImport("kwatcher", kwatcher);
    // 3rd Party:
    kwatcher_event_library.addImport("pg", pg);
    kwatcher_event_library.addImport("uuid", uuid);
}
