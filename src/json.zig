const std = @import("std");

// Given 2 json values -> compare them.
pub fn eql(a: std.json.Value, b: std.json.Value) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => a.bool == b.bool,
        .integer => a.integer == b.integer,
        .float => a.float == b.float,
        .string => std.mem.eql(u8, a.string, b.string),
        .number_string => std.mem.eql(u8, a.number_string, b.number_string),
        .array => {
            const arr_a = a.array;
            const arr_b = b.array;
            if (arr_a.items.len != arr_b.items.len) return false;

            for (arr_a.items, arr_b.items) |el_a, el_b| {
                // For our implementation we are going to assume that array order matters for equality
                if (!eql(el_a, el_b)) return false;
            }

            return true;
        },
        .object => {
            const obj_a = a.object;
            const obj_b = b.object;
            if (obj_a.count() != obj_b.count()) return false;

            for (obj_a.keys()) |key| {
                if (!obj_b.contains(key)) return false; // This is probably less efficient than just getting it and checking for null. More readable though.
                const val_a = obj_a.get(key).?;
                const val_b = obj_b.get(key).?;
                if (!eql(val_a, val_b)) return false;
            }

            return true;
        },
    };
}
