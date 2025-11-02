const std = @import("std");
const log = std.log.scoped(.kclient);
const pg = @import("pg");
const uuid = @import("uuid");
const kwatcher = @import("kwatcher");

const models = @import("../models.zig");

const KClientRepo = @This();

conn: *pg.Conn,

pub const KClientRow = struct {
    id: []const u8, // UUID
    kname: []const u8, // TEXT
    kversion: []const u8, // TEXT
    host: []const u8, // TEXT
};

pub fn init(pool: *pg.Pool) !KClientRepo {
    return .{
        .conn = try pool.acquire(),
    };
}

pub fn deinit(self: *KClientRepo) void {
    self.conn.release();
}

pub const FromPool = struct {
    pool: *pg.Pool,
    pub fn init(pool: *pg.Pool) !FromPool {
        return .{
            .pool = pool,
        };
    }

    pub fn yield(self: *FromPool) !KClientRepo {
        return .init(self.pool);
    }
};

pub fn getOrCreate(self: *KClientRepo, arena: *kwatcher.mem.InternalArena, client: kwatcher.schema.ClientInfo, user: kwatcher.schema.UserInfo) !KClientRow {
    const alloc = arena.allocator();
    const row = self.conn.rowOpts(
        "select * from kclient where kname = $1 and kversion = $2 and host = $3 limit 1",
        .{ client.name, client.version, user.hostname },
        .{ .column_names = true },
    ) catch |e| switch (e) {
        error.PG => {
            if (self.conn.err) |pge| {
                log.err(
                    "[{s}] Encountered an error ({s}) while retrieving a client: \n{s}\n",
                    .{ pge.severity, pge.code, pge.message },
                );
            } else {
                log.err("Encountered an unknown error while retrieving a client.\n", .{});
            }
            return e;
        },
        else => return e,
    };
    if (row) |_| {
        var found = row.?;
        const data = try found.to(KClientRow, .{
            .map = .name,
            .allocator = alloc,
        });
        found.deinit() catch {};

        return data;
    } else {
        const id = uuid.v7.new();
        const urn = uuid.urn.serialize(id);

        _ = try self.conn.exec(
            "INSERT INTO kclient (id, kname, kversion, host) VALUES ($1, $2, $3, $4)",
            .{ urn, client.name, client.version, user.hostname },
        );

        return .{
            .id = &urn,
            .kname = client.name,
            .kversion = client.version,
            .host = user.hostname,
        };
    }
}

pub fn getClients(self: *KClientRepo, allocator: std.mem.Allocator, ef: models.EventFilters) !std.ArrayList([]const u8) {
    const start = "select distinct kname from kclient c ";
    const end = " order by kname";

    var writer = std.io.Writer.Allocating.init(allocator);
    const wi = &writer.writer;

    var eq = ef;
    eq.clients = null;

    try buildEventFilterQuery(
        wi,
        eq,
        start,
        end,
        1,
    );

    const query = try writer.toOwnedSlice();
    defer allocator.free(query);

    var stmt = try pg.Stmt.init(
        self.conn,
        .{ .allocator = allocator },
    );
    errdefer stmt.deinit();

    stmt.prepare(query, null) catch |e| switch (e) {
        error.PG => {
            if (self.conn.err) |pge| {
                log.err(
                    "[{s}] Encountered an error ({s}) while preparing client query: \n{s}\n",
                    .{ pge.severity, pge.code, pge.message },
                );
            } else {
                log.err("Encountered an unknown error while preparing client query.\n", .{});
            }
            return e;
        },
        else => return e,
    };

    try bindEventFilterQueryParams(&stmt, eq);

    const result = stmt.execute() catch |e| switch (e) {
        error.PG => {
            if (self.conn.err) |pge| {
                log.err(
                    "[{s}] Encountered an error ({s}) while retrieving a client: \n{s}\n",
                    .{ pge.severity, pge.code, pge.message },
                );
            } else {
                log.err("Encountered an unknown error while retrieving a client.\n", .{});
            }
            return e;
        },
        else => return e,
    };
    defer result.deinit();

    var clients = try std.ArrayList([]const u8).initCapacity(allocator, result._values.len);

    while (try result.next()) |row| {
        try clients.append(allocator, row.get([]const u8, 0));
    }

    return clients;
}

pub fn getHosts(
    self: *KClientRepo,
    allocator: std.mem.Allocator,
    ef: models.EventFilters,
) !std.ArrayList([]const u8) {
    const start = "select distinct host from kclient c ";
    const end = " order by host";

    var writer = std.io.Writer.Allocating.init(allocator);
    const wi = &writer.writer;

    var eq = ef;
    eq.hosts = null;

    try buildEventFilterQuery(
        wi,
        eq,
        start,
        end,
        1,
    );

    const query = try writer.toOwnedSlice();
    defer allocator.free(query);

    var stmt = try pg.Stmt.init(
        self.conn,
        .{ .allocator = allocator },
    );
    errdefer stmt.deinit();

    stmt.prepare(query, null) catch |e| switch (e) {
        error.PG => {
            if (self.conn.err) |pge| {
                log.err(
                    "[{s}] Encountered an error ({s}) while preparing client query: \n{s}\n",
                    .{ pge.severity, pge.code, pge.message },
                );
            } else {
                log.err("Encountered an unknown error while preparing client query.\n", .{});
            }
            return e;
        },
        else => return e,
    };

    try bindEventFilterQueryParams(&stmt, eq);

    const result = stmt.execute() catch |e| switch (e) {
        error.PG => {
            if (self.conn.err) |pge| {
                log.err(
                    "[{s}] Encountered an error ({s}) while retrieving a client: \n{s}\n",
                    .{ pge.severity, pge.code, pge.message },
                );
            } else {
                log.err("Encountered an unknown error while retrieving a client.\n", .{});
            }
            return e;
        },
        else => return e,
    };
    defer result.deinit();

    var clients = try std.ArrayList([]const u8).initCapacity(allocator, result._values.len);

    while (try result.next()) |row| {
        try clients.append(allocator, row.get([]const u8, 0));
    }

    return clients;
}

fn buildEventFilterQuery(
    writer: *std.io.Writer,
    eq: models.EventFilters,
    start: []const u8,
    end: []const u8,
    filterIndexStart: u8,
) !void {
    try writer.writeAll(start);
    const hasEventTypeFilter = eq.event_types != null and !std.mem.eql(u8, eq.event_types.?, "All");
    const hasClientFilter = eq.clients != null and !std.mem.eql(u8, eq.clients.?, "All");
    const hasHostFilter = eq.hosts != null and !std.mem.eql(u8, eq.hosts.?, "All");
    if (hasEventTypeFilter) {
        try writer.writeAll(" join kevent e ON c.id = e.kclient");
    }

    var startedFilter = false;
    var filterIndex: u8 = filterIndexStart;

    if (hasEventTypeFilter) {
        if (startedFilter) {
            try writer.writeAll(" and ");
        } else {
            try writer.writeAll(" where ");
            startedFilter = true;
        }
        try writer.writeAll("e.event_type = $");
        try writer.writeAll(&std.fmt.digits2(filterIndex));
        filterIndex += 1;
    }

    if (hasClientFilter) {
        if (startedFilter) {
            try writer.writeAll(" and ");
        } else {
            try writer.writeAll(" where ");
            startedFilter = true;
        }
        try writer.writeAll("c.kname = $");
        try writer.writeAll(&std.fmt.digits2(filterIndex));
        filterIndex += 1;
    }

    if (hasHostFilter) {
        if (startedFilter) {
            try writer.writeAll(" and ");
        } else {
            try writer.writeAll(" where ");
            startedFilter = true;
        }
        try writer.writeAll("c.host = $");
        try writer.writeAll(&std.fmt.digits2(filterIndex));
        filterIndex += 1;
    }

    try writer.writeAll(end);
    try writer.flush();
}

fn bindEventFilterQueryParams(stmt: *pg.Stmt, eq: models.EventFilters) !void {
    const hasEventTypeFilter = eq.event_types != null and !std.mem.eql(u8, eq.event_types.?, "All");
    const hasClientFilter = eq.clients != null and !std.mem.eql(u8, eq.clients.?, "All");
    const hasHostFilter = eq.hosts != null and !std.mem.eql(u8, eq.hosts.?, "All");

    if (hasEventTypeFilter) {
        try stmt.bind(eq.event_types.?);
    }

    if (hasClientFilter) {
        try stmt.bind(eq.clients.?);
    }

    if (hasHostFilter) {
        try stmt.bind(eq.hosts.?);
    }
}
