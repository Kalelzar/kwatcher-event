const pg = @import("pg");

// TODO: Add migration system
pub fn initialize(conn: *pg.Conn) !void {
    _ = try conn.exec(
        \\ CREATE TABLE IF NOT EXISTS kclient (
        \\ id UUID PRIMARY KEY,
        \\ kname TEXT NOT NULL,
        \\ kversion TEXT NOT NULL,
        \\ host TEXT NOT NULL,
        \\ UNIQUE (kversion, kname, host)
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS kevent (
        \\ id UUID PRIMARY KEY,
        \\ kclient UUID NOT NULL,
        \\ user_id TEXT NOT NULL,
        \\ event_type TEXT NOT NULL,
        \\ start_time TIMESTAMPTZ NOT NULL,
        \\ end_time TIMESTAMPTZ NOT NULL,
        \\ properties JSONB NOT NULL,
        \\ UNIQUE(user_id, event_type, end_time),
        \\ FOREIGN KEY (kclient) REFERENCES kclient(id) ON DELETE CASCADE
        \\);
        \\CREATE INDEX IF NOT EXISTS kevents_span ON kevent (start_time, end_time);
        \\CREATE INDEX IF NOT EXISTS latest_kevents_of ON kevent (user_id, event_type, end_time DESC);
    , .{});
}
