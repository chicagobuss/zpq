# Design: SchemaTree (B1.0)

**Status:** designed (2026-05-04 evening), not yet implemented.

**Replaces:** the implicit "flat thrift list IS the in-memory schema"
representation that breaks for nested schemas.

**Predecessor reading:** `docs/journal/2026-05.md` 21:41 entry — what
fabricated nested fixtures revealed about `findColumnIndex` and
`cloneProjectedSchema` silently producing wrong outputs for nested
columns.

---

## Why

ZPQ today uses `meta.schema: ArrayList(SchemaElement)` — the flat
thrift list — as both the wire format AND the in-memory representation.
For flat schemas this is fine (each non-root element is a leaf;
`schema[i+1]` = column chunk `i`). For nested schemas it's structurally
wrong: GROUP nodes appear in the list and shift indexing; user-facing
column names map to a SET of leaves, not one; output schemas need GROUP
ancestors preserved with proper LIST/MAP annotations.

**Both reference implementations we have access to do the same thing:**

- **Hardwood** (`references/hardwood/.../schema/SchemaNode.java`,
  `FileSchema.java`): sealed-interface tree of `PrimitiveNode | GroupNode`,
  built once. Cached `columnPathToIndex: Map<String, int>` for O(1)
  lookups. Top-level field lookup via `getField(name)` walks tree
  children.
- **Polars / parquet2**
  (`references/polars/crates/polars-parquet/.../schema_descriptor.rs`):
  `SchemaDescriptor { fields: Vec<ParquetType>, leaves: Vec<ColumnDescriptor> }`.
  Recursive `ParquetType` for the tree, flat `leaves` array for
  column-chunk-index access.

The pattern is universal: **the flat thrift list is a serialisation
format, not an in-memory representation.** Every parquet engine builds
a tree once, walks it for navigation, serialises back to flat thrift
only at write time.

ZPQ shipping with the flat list as in-memory was a leak from "flat-only"
shortcuts in earlier phases. B1.0 fixes the foundation; B1.x/y/z
(column resolution, schema projection, multi-leaf dispatch) all become
small once the foundation is right.

## What

A new module `core/parquet/schema_tree.zig` providing:

```zig
pub const SchemaTree = struct {
    /// Tagged-union node: each tree position is either a primitive
    /// (= leaf = one column chunk per row group) or a group (struct,
    /// list, map, or variant container).
    pub const Node = union(enum) {
        primitive: PrimitiveNode,
        group: GroupNode,
    };

    pub const PrimitiveNode = struct {
        name: []const u8,             // arena-owned; copy of source
        path: []const []const u8,     // arena-owned; full path from root
        type: schema.Type,
        repetition: schema.FieldRepetitionType,
        logical_type: ?schema.LogicalType,
        converted_type: ?schema.ConvertedType,
        column_index: u32,            // chunk index in row groups
        max_def: u8,                  // pre-computed
        max_rep: u8,                  // pre-computed
    };

    pub const GroupNode = struct {
        name: []const u8,
        repetition: schema.FieldRepetitionType,
        logical_type: ?schema.LogicalType,
        converted_type: ?schema.ConvertedType,
        children: []const Node,       // arena-owned
        kind: GroupKind,              // cached from logical/converted_type
    };

    pub const GroupKind = enum {
        struct_,        // plain struct
        list,           // LIST (children = [REPEATED list { element }])
        map,            // MAP (children = [REPEATED key_value { key, value }])
        map_key_value,  // legacy MAP_KEY_VALUE (rarely emitted nowadays)
        variant,        // parquet 2.13 logical type, future-proofing
    };

    root: GroupNode,
    /// Flat, column-chunk-order array of leaves. `leaves[i]` is the
    /// PrimitiveNode for column chunk i in any row group. This array
    /// is what existing fastpath / metadata code that thinks in
    /// "column index" continues to use, but now via the tree-derived
    /// PrimitiveNode (paths and levels correct).
    leaves: []const PrimitiveNode,
    /// O(1) lookup from a dot-joined path (e.g. "events.list.element.ts")
    /// to the column-chunk index. Keys are arena-owned (joined once
    /// during construction).
    path_to_leaf: std.StringHashMapUnmanaged(u32),

    pub const Error = error{
        EmptySchema,
        MalformedListAnnotation,
        MalformedMapAnnotation,
        DuplicatePath,
        TruncatedFlat,
    } || std.mem.Allocator.Error;

    /// Build the tree from a parsed thrift schema list. Single DFS
    /// pass; all internal allocations on `arena`.
    pub fn build(
        arena: std.mem.Allocator,
        flat: []const schema.SchemaElement,
    ) Error!SchemaTree;

    /// Resolve a top-level user-facing path (single name like "events"
    /// or dotted "events.list.element.ts") to a SET of column-chunk
    /// indices. For top-level groups, returns all descendant leaves.
    /// For a single leaf, returns one index.
    pub fn resolveTopLevel(
        self: *const SchemaTree,
        arena: std.mem.Allocator,
        name: []const u8,
    ) Error![]const u32;

    /// Build a new SchemaTree containing only the kept leaves (and
    /// their ancestor groups). Output preserves logical/converted
    /// type annotations so downstream readers reassemble lists/maps.
    pub fn projectSubset(
        self: *const SchemaTree,
        arena: std.mem.Allocator,
        kept_leaf_indices: []const u32,
    ) Error!SchemaTree;

    /// Serialise back to a flat thrift schema list, in DFS order,
    /// suitable for `FileMetaData.schema`. Re-uses the input arena.
    pub fn writeFlatThrift(
        self: *const SchemaTree,
        arena: std.mem.Allocator,
    ) Error!std.ArrayListUnmanaged(schema.SchemaElement);

    /// Custom debug formatter — produces canonical `parquet-tools
    /// schema`-style output (matches hardwood's `schema` subcommand).
    pub fn format(self: SchemaTree, writer: *std.Io.Writer) std.Io.Writer.Error!void;
};
```

## How

### Construction (DFS pass)

A small `Builder` struct threads the cursor over the flat thrift list
and the running `(def, rep)` state:

```zig
const Builder = struct {
    arena: std.mem.Allocator,
    flat: []const schema.SchemaElement,
    pos: usize = 0,
    leaves: std.ArrayList(PrimitiveNode) = .empty,
    path_stack: std.ArrayList([]const u8) = .empty,
    path_to_leaf: std.StringHashMapUnmanaged(u32) = .{},

    fn buildNode(self: *Builder, def: u8, rep: u8) Error!Node { ... }
};
```

Per-element walk applies the parquet-spec accumulation rule:

```
def_here = def + (if rep_type == REQUIRED then 0 else 1)
rep_here = rep + (if rep_type == REPEATED then 1 else 0)
```

If the element has `num_children == null` (or 0) → emit a
`PrimitiveNode` with `column_index = leaves.len` (assigned in DFS
order, which matches column-chunk order). Push name onto path_stack
before recursing into children, pop after. Join path on each leaf
emit and store in `path_to_leaf`.

For GROUP elements, classify their `kind` from `converted_type` /
`logical_type`:

- `LIST` annotation → `kind = .list`. Validate children = `[REPEATED
  group { element }]` shape.
- `MAP` or `MAP_KEY_VALUE` annotation → `kind = .map`. Validate
  children = `[REPEATED group { key, value }]` shape.
- Otherwise → `kind = .struct_`.

Validation failures map to specific errors so debugging is grep-able.

### Walked example (`nested_edges.parquet`)

Schema list flat (DFS order from thrift):

```
[0] root          GROUP   num_children=6
[1] id            INT64   OPTIONAL  → leaf 0  path=["id"]               max_def=1 max_rep=0
[2] score         INT32   OPTIONAL  → leaf 1  path=["score"]            max_def=1 max_rep=0
[3] tags          GROUP   OPTIONAL  num_children=1, conv=LIST
[4] tags.list     GROUP   REPEATED  num_children=1
[5] tags...item   BYTE_ARRAY OPTIONAL → leaf 2 path=["tags","list","element"]    max_def=3 max_rep=1
[6] meta          GROUP   OPTIONAL  num_children=2
[7] meta.k        BYTE_ARRAY OPTIONAL → leaf 3 path=["meta","k"]                 max_def=2 max_rep=0
[8] meta.v        INT32   OPTIONAL  → leaf 4  path=["meta","v"]                  max_def=2 max_rep=0
[9] events        GROUP   OPTIONAL  num_children=1, conv=LIST
[10] events.list  GROUP   REPEATED  num_children=1
[11] events...el  GROUP   OPTIONAL  num_children=2
[12] events...ts  INT64   OPTIONAL  → leaf 5  path=["events","list","element","ts"]   max_def=4 max_rep=1
[13] events...cd  INT32   OPTIONAL  → leaf 6  path=["events","list","element","code"] max_def=4 max_rep=1
[14] counts       GROUP   OPTIONAL  num_children=1, conv=MAP
[15] counts.kv    GROUP   REPEATED  num_children=2
[16] counts.kv.k  BYTE_ARRAY REQUIRED → leaf 7 path=["counts","key_value","key"]    max_def=2 max_rep=1
[17] counts.kv.v  INT32   OPTIONAL  → leaf 8  path=["counts","key_value","value"]   max_def=3 max_rep=1
```

Tree built in one DFS pass. `leaves[].column_index` == position in
this listing. Hardwood's `info` command on the same file reports
identical max_def/max_rep at every leaf, confirming the algorithm.

### Resolving user-supplied projection

`resolveTopLevel("events")`:
1. Walk root's children, find `name == "events"`. It's a GroupNode.
2. Walk that subtree DFS, collect every `column_index` from
   PrimitiveNode descendants → `[5, 6]`.
3. Return.

`resolveTopLevel("events.list.element.ts")`:
1. Hit `path_to_leaf["events.list.element.ts"]` directly → `[5]`.
2. (Lookup hits the hashmap; tree walk only fires for top-level group
   shorthand.)

`resolveTopLevel("nonexistent")`:
1. No top-level child match, no path_to_leaf hit → return empty slice
   or error (caller decides). API choice: return empty + let caller
   error with their own context (consistent with how
   `findColumnIndex` returns null today).

### Schema projection

`projectSubset(kept = [0, 5, 6])` (id + both events leaves):
1. For each kept leaf index, walk up via the `path` field — each
   PrimitiveNode already carries its full path from root.
2. Build a set of "needed" path prefixes: `{["id"], ["events"],
   ["events","list"], ["events","list","element"],
   ["events","list","element","ts"],
   ["events","list","element","code"]}`.
3. Walk the source tree DFS; emit any node whose path is in the
   needed set. For groups, only emit children that have any kept
   descendant.
4. Re-number `column_index` densely in the new tree (kept-leaves
   get indices 0..K-1 in DFS order).
5. Return new SchemaTree with same construction invariants.

Output schema correctly preserves `tags (LIST)` annotations and the
`REPEATED list` group structure — downstream readers reassemble lists.

### Round-trip back to flat thrift

`writeFlatThrift` is a recursive walk that emits SchemaElements in DFS
order. The current `schema.SchemaElement.write` function already
exists; we just feed it nodes in the right order. The output ArrayList
matches what `FileMetaData.schema` expects.

## Migration of existing call sites

Every site that today touches `meta.schema.items[N]` directly gets
rewritten:

| Today                                                    | After                                                          |
| -------------------------------------------------------- | -------------------------------------------------------------- |
| `metadata.findColumnIndex(name)` (broken for nested)     | `tree.resolveTopLevel(name)` returning `[]const u32`           |
| `meta.schema.items[ci + 1]` for leaf SchemaElement       | `tree.leaves[ci]` (PrimitiveNode with everything pre-cached)   |
| `meta.getColumnLevels(path_arr)`                         | `tree.leaves[ci].max_def` / `.max_rep` directly                |
| `meta.getColumnSchema(path)`                             | `tree.leaves[ci]` directly (or path lookup)                    |
| `cloneSchemaAsRequired` / `cloneProjectedSchema`         | `tree.projectSubset(kept_indices)` + `writeFlatThrift`         |
| Filter parser building flat path                         | Filter parser feeds `tree.resolveTopLevel(col_name)`           |

`schema.zig` (the thrift representation) stays. It's the wire format.
`metadata.open` continues to parse the thrift but now also calls
`SchemaTree.build` and stores the tree on `FileMetaData`. Existing
flat-list code paths can keep using the flat list during the
transition; new nested-aware code uses the tree. Eventually the flat
list lives only inside the tree, accessed via `writeFlatThrift` at
footer-write time.

## Zig 0.16 design choices, with rationale

**`union(enum)` for `Node`.** Tagged unions are zero-cost and Zig's
regular `switch` on them is compile-time exhaustive — adding a
variant later (e.g., `variant` for parquet 2.13's Variant logical
type) makes every walker fail compilation until updated. No `inline
switch` needed; regular `switch` already enforces exhaustiveness.
(Verified empirically: probe at 21:48 showed `nameOfMissing` failed
to compile when one variant was unhandled.)

**Single `arena: std.mem.Allocator` lifetime.** Tree built in
`metadata.open`, lives until the file is closed, dies with the
arena. No per-node `deinit`, no recursion, no cycle concerns. Matches
existing ZPQ allocator discipline. Better than hardwood's GC-tracked
nodes for our use case.

**`std.StringHashMapUnmanaged(u32)` for `path_to_leaf`.** Verified
in 0.16 stdlib. Unmanaged variant keeps `SchemaTree` copy-cheap
(no embedded allocator). Keys are dot-joined paths, arena-owned at
build time. O(1) lookup.

**Custom `format(self, writer: *std.Io.Writer)` method.** 0.16's
new format protocol is single-writer, no comptime fmt string —
cleaner than 0.15. Implementing it on `SchemaTree` lets us
`std.debug.print("{}", .{tree})` and get parquet-tools-style output,
useful for the conform tool's debug output and for journal entries.

**Inline `[]const Node` slice for `children`.** Each Node is sized by
the largest variant (~64 bytes). For 1000-leaf schemas that's 64KB —
fine. If schemas ever get pathological we can switch to
`[]const *const Node` (slice of pointers into a separately-allocated
node arena). For now, value-semantics children keep the API simple.

**Plain `[]PrimitiveNode` for `leaves`, not `MultiArrayList`.**
Considered SoA. Our access patterns (random-access by index,
sequential walks that touch most fields, hash-map lookups) don't
benefit. Simpler is better here.

**Builder struct for construction.** Single function with arena +
cursor + accumulators. Cleaner than threading state through recursive
calls or using a global walker. Returns the SchemaTree value.

**Explicit error set.** `pub const Error = error{ ... }` documents
the failure surface and lets callers distinguish "your input was
malformed" from "we ran out of memory."

## Out of scope for B1.0

- Filter AST changes (still uses single-name `col_idx`). B1.x will
  rework that to use full paths or leaf-index sets.
- The Lambda main's projection-resolution rewrite. That's B1.x/y/z.
- Decode-side rep-level support is already done (B1b shipped) and
  doesn't depend on the tree — it operates per leaf chunk.

## Acceptance criteria

B1.0 is "done" when:

1. `metadata.open` builds and stores a SchemaTree alongside the flat
   schema.
2. `tree.leaves` returns the same set of column-chunk-aligned leaves
   as the existing flat-walk-with-luck pattern, AND with correct
   max_def / max_rep / path on every leaf, validated against
   hardwood's `info` output for every file in
   `data/parquet-testing/data/`.
3. `tree.resolveTopLevel("events")` returns `[5, 6]` on
   `nested_edges.parquet` (and similar correct sets on every other
   nested file we own).
4. `tree.projectSubset([5, 6])` followed by `writeFlatThrift`
   produces a schema list that pyarrow + duckdb + hardwood all
   parse correctly.
5. All 148 existing unit tests pass; conformance corpus pass count
   doesn't regress (B1.0 adds no decode/encode behaviour, just
   replaces representations).
6. `tree.format` produces output that round-trips through hardwood's
   schema parser (i.e., feeding our format output to a parquet writer
   reconstructs the same tree).

After B1.0 lands, B1.x (column resolution rewrite) becomes a 50-LoC
function. B1.y (schema projection) becomes a 100-LoC function. B1.z
(multi-leaf dispatch in the lambda main) becomes a refactor of
`kept_in_order` to track tree-derived chunk-index sets. The hardest
part is B1.0; everything else is downstream.
