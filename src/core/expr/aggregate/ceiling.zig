//! The private memory ceiling a group table lends to the DISTINCT sets it holds.
//!
//! A set spilled onto a private ceiling is charged on the table that owns its group. The table holds the sets in its
//! accumulators, so it sits above them: it embeds a `Ceiling`, and a set sees only this interface, never the table.

/// Embedded in the table at 16-byte alignment, so an inline DISTINCT acc can keep its address with bit 3 set
/// (`DistinctAcc.ceiling_flag`). The table must not move while an acc or a set refers to it.
pub const Ceiling = struct {
    vtable: *const VTable,

    pub const VTable = struct {
        /// Count `n` bytes against the ceiling, or refuse when they would pass it.
        charge: *const fn (*Ceiling, usize) error{ExceededMemoryBudget}!void,
        release: *const fn (*Ceiling, usize) void,
    };

    pub fn charge(self: *Ceiling, n: usize) error{ExceededMemoryBudget}!void {
        return self.vtable.charge(self, n);
    }

    pub fn release(self: *Ceiling, n: usize) void {
        self.vtable.release(self, n);
    }
};
