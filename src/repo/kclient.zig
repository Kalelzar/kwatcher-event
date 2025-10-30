const std = @import("std");
const log = std.log.scoped(.kclient);
const pg = @import("pg");
const uuid = @import("uuid");
const kwatcher = @import("kwatcher");

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

pub fn getClients(self: *KClientRepo, allocator: std.mem.Allocator) !std.ArrayList([]const u8) {
    const result = self.conn.queryOpts(
        "select distinct kname from kclient order by kname",
        .{},
        .{ .allocator = allocator },
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
    defer result.deinit();

    var clients = try std.ArrayList([]const u8).initCapacity(allocator, result._values.len);

    while (try result.next()) |row| {
        try clients.append(allocator, row.get([]const u8, 0));
    }

    return clients;
}

pub fn getHosts(self: *KClientRepo, allocator: std.mem.Allocator) !std.ArrayList([]const u8) {
    const result = self.conn.queryOpts(
        "select distinct host from kclient order by host",
        .{},
        .{ .allocator = allocator },
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
    defer result.deinit();

    var clients = try std.ArrayList([]const u8).initCapacity(allocator, result._values.len);

    while (try result.next()) |row| {
        try clients.append(allocator, row.get([]const u8, 0));
    }

    return clients;
}
