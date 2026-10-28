package dev.zpq.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves input Parquet paths, caches Parquet footer metadata (schema and row-group counts),
 * and splits row groups across Spark InputPartitions (following tools/serverless/fanout.py).
 */
public final class ZpqPaths {
    private ZpqPaths() {}

    public record FileMeta(String path, long lastModified, long length, int rowGroups, StructType schema) implements Serializable {}

    public record RowGroupSlice(String filePath, int startRg, int endRg) implements Serializable {}

    private static final ConcurrentHashMap<String, FileMeta> META_CACHE = new ConcurrentHashMap<>();

    public static List<String> resolveFiles(Map<String, String> options) {
        List<String> rawPaths = new ArrayList<>();
        String singlePath = options.get("path");
        if (singlePath != null && !singlePath.isEmpty()) {
            rawPaths.add(singlePath);
        }
        String pathsJson = options.get("paths");
        if (pathsJson != null && !pathsJson.isEmpty()) {
            // Simple JSON array of strings parser
            int idx = 0;
            while (idx < pathsJson.length()) {
                int q1 = pathsJson.indexOf('"', idx);
                if (q1 < 0) break;
                int q2 = pathsJson.indexOf('"', q1 + 1);
                if (q2 < 0) break;
                String p = pathsJson.substring(q1 + 1, q2);
                if (!rawPaths.contains(p)) {
                    rawPaths.add(p);
                }
                idx = q2 + 1;
            }
        }
        if (rawPaths.isEmpty()) {
            throw new IllegalArgumentException("No input path specified for zpq data source");
        }

        Configuration conf = new Configuration();
        Set<String> resolved = new TreeSet<>();
        for (String raw : rawPaths) {
            try {
                File localFile = new File(raw);
                if (localFile.exists() && localFile.isDirectory()) {
                    File[] children = localFile.listFiles((d, name) -> name.endsWith(".parquet") && !name.startsWith(".") && !name.startsWith("_"));
                    if (children != null) {
                        for (File c : children) {
                            resolved.add(c.getAbsolutePath());
                        }
                    }
                    continue;
                }
                if (localFile.exists() && localFile.isFile()) {
                    resolved.add(localFile.getAbsolutePath());
                    continue;
                }
                Path hPath = new Path(raw);
                FileSystem fs = hPath.getFileSystem(conf);
                FileStatus[] statuses = fs.globStatus(hPath);
                if (statuses != null) {
                    for (FileStatus st : statuses) {
                        if (st.isDirectory()) {
                            FileStatus[] sub = fs.listStatus(st.getPath(), p -> p.getName().endsWith(".parquet") && !p.getName().startsWith(".") && !p.getName().startsWith("_"));
                            for (FileStatus s2 : sub) {
                                resolved.add(Path.getPathWithoutSchemeAndAuthority(s2.getPath()).toString());
                            }
                        } else if (st.getPath().getName().endsWith(".parquet")) {
                            resolved.add(Path.getPathWithoutSchemeAndAuthority(st.getPath()).toString());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to resolve path: " + raw, e);
            }
        }
        if (resolved.isEmpty()) {
            throw new IllegalArgumentException("No .parquet files found for paths: " + rawPaths);
        }
        return new ArrayList<>(resolved);
    }

    public static FileMeta getFileMeta(String filePath) {
        File f = new File(filePath);
        long mtime = f.lastModified();
        long len = f.length();
        boolean binaryAsString = SQLConf.get().isParquetBinaryAsString();
        String cacheKey = filePath + "|" + binaryAsString;
        FileMeta cached = META_CACHE.get(cacheKey);
        if (cached != null && cached.lastModified() == mtime && cached.length() == len) {
            return cached;
        }
        Configuration conf = new Configuration();
        try (ParquetFileReader reader = ParquetFileReader.open(
            HadoopInputFile.fromPath(new Path(filePath), conf)
        )) {
            int rgs = reader.getRowGroups().size();
            ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(SQLConf.get());
            StructType schema = converter.convert(reader.getFileMetaData().getSchema());
            FileMeta meta = new FileMeta(filePath, mtime, len, rgs, schema);
            META_CACHE.put(cacheKey, meta);
            return meta;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Parquet footer from " + filePath, e);
        }
    }

    public static StructType inferSchema(CaseInsensitiveStringMap options) {
        List<String> files = resolveFiles(options);
        return getFileMeta(files.get(0)).schema();
    }

    /**
     * Splits input files' row groups into contiguous (filePath, startRg, endRg) slices,
     * following tools/serverless/fanout.py's split(counts, workers).
     */
    public static List<RowGroupSlice> splitRowGroups(List<String> files, int maxWorkers) {
        record Unit(String filePath, int rg) {}
        List<Unit> units = new ArrayList<>();
        for (String f : files) {
            int n = getFileMeta(f).rowGroups();
            for (int rg = 0; rg < n; rg++) {
                units.add(new Unit(f, rg));
            }
        }
        if (units.isEmpty()) {
            return List.of(new RowGroupSlice(files.get(0), 0, 0));
        }
        int n = Math.min(Math.max(1, maxWorkers), units.size());
        List<RowGroupSlice> slices = new ArrayList<>();
        int total = units.size();
        for (int k = 0; k < n; k++) {
            int from = (k * total) / n;
            int to = ((k + 1) * total) / n;
            String curFile = null;
            int startRg = 0;
            int endRg = 0;
            for (int uIdx = from; uIdx < to; uIdx++) {
                Unit u = units.get(uIdx);
                if (curFile != null && curFile.equals(u.filePath())) {
                    endRg = u.rg() + 1;
                } else {
                    if (curFile != null) {
                        slices.add(new RowGroupSlice(curFile, startRg, endRg));
                    }
                    curFile = u.filePath();
                    startRg = u.rg();
                    endRg = u.rg() + 1;
                }
            }
            if (curFile != null) {
                slices.add(new RowGroupSlice(curFile, startRg, endRg));
            }
        }
        return slices;
    }
}
