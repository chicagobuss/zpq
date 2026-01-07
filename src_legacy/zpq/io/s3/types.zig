const std = @import("std");

/// Unified credentials for AWS SigV4
pub const Credentials = struct {
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,
};

/// Unified S3 Configuration for both Sync and Async stacks
pub const S3Config = struct {
    credentials: ?Credentials = null,
    region: []const u8 = "us-east-1",
    endpoint: ?[]const u8 = null, // Custom endpoint (e.g. MinIO)
};

