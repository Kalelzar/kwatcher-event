const klib = @import("klib");

pub const Cursor = @import("models/query.zig");
pub const EventFilters = @import("models/event_filters.zig");

pub const PaginatedEventsQuery = klib.meta.MergeStructs(Cursor, EventFilters);
