// Reports, per equality value, how many rows Hardwood returns and which row groups it dropped by which mechanism.
//
// Hardwood exposes no pruning counters through its API; the only observable record of row-group pruning is its JFR
// events, so this harness runs an in-process JFR recording around each filtered read and counts the events after:
//   dev.hardwood.RowGroupFilter            one per read plan: totalRowGroups, rowGroupsSkipped (statistics)
//   dev.hardwood.RowGroupBloomFilter       one per row group dropped by a Bloom filter (field rowGroupIndex)
//   dev.hardwood.RowGroupDictionaryFilter  one per row group dropped by its dictionary (field rowGroupIndex)
// Field names verified against core/src/main/java/dev/hardwood/jfr/*Event.java.
//
// Usage:
//   java -cp <hardwood-core + codecs>:<classes> HwBloom --file F --col C --type T --values v1,v2,...
//   T in int,long,float,double,str,date (date = ISO yyyy-mm-dd against an INT32 DATE column)
//
// Output (stdout), one line per value:
//   VALUE <v> ROWS <n> BLOOM_PRUNED <k> DICT_PRUNED <j> STATS_PRUNED <s> ROW_GROUPS <total>

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

public final class HwBloom {

    static final String EV_STATS = "dev.hardwood.RowGroupFilter";
    static final String EV_BLOOM = "dev.hardwood.RowGroupBloomFilter";
    static final String EV_DICT = "dev.hardwood.RowGroupDictionaryFilter";

    record Result(long rows, int bloom, int dict, int stats, int totalFromEvent) {}

    static FilterPredicate eq(String col, String type, String v) {
        return switch (type) {
            case "int" -> FilterPredicate.eq(col, Integer.parseInt(v));
            case "long" -> FilterPredicate.eq(col, Long.parseLong(v));
            case "float" -> FilterPredicate.eq(col, Float.parseFloat(v));
            case "double" -> FilterPredicate.eq(col, Double.parseDouble(v));
            case "str" -> FilterPredicate.eq(col, v);
            case "date" -> FilterPredicate.eq(col, LocalDate.parse(v));
            default -> throw new IllegalArgumentException("unknown --type " + type);
        };
    }

    static Result probe(HardwoodContext ctx, Path file, String col, FilterPredicate pred, Path jfrOut)
            throws Exception {
        long rows = 0;
        try (Recording rec = new Recording()) {
            // Bloom/dictionary drop events are instant events; the default threshold would still record them, but
            // pinning it to zero keeps the count independent of any JFR settings the JVM was launched with.
            rec.enable(EV_STATS).withoutThreshold();
            rec.enable(EV_BLOOM).withoutThreshold();
            rec.enable(EV_DICT).withoutThreshold();
            rec.start();
            // A fresh reader per value so no state from a previous probe can stand in for this read's pruning.
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), ctx);
                 ColumnReader cr = reader.buildColumnReader(col).filter(pred).build()) {
                // Bloom and dictionary drops are decided (and their events emitted) only when the read reaches the
                // row group, so the read must be drained to the end for the counts to be complete.
                while (cr.nextBatch()) {
                    rows += cr.getRecordCount();
                }
            }
            rec.stop();
            rec.dump(jfrOut);
        }

        int stats = 0, totalFromEvent = -1, bloomEvents = 0, dictEvents = 0;
        Set<Integer> bloomRg = new HashSet<>(), dictRg = new HashSet<>();
        for (RecordedEvent e : RecordingFile.readAllEvents(jfrOut)) {
            switch (e.getEventType().getName()) {
                case EV_STATS -> {
                    stats += e.getInt("rowGroupsSkipped");
                    totalFromEvent = Math.max(totalFromEvent, e.getInt("totalRowGroups"));
                }
                case EV_BLOOM -> { bloomEvents++; bloomRg.add(e.getInt("rowGroupIndex")); }
                case EV_DICT -> { dictEvents++; dictRg.add(e.getInt("rowGroupIndex")); }
                default -> { }
            }
        }
        // Report distinct row groups, but say so if Hardwood ever emits a drop twice for one row group, since that
        // would mean the documented one-event-per-dropped-row-group contract does not hold for this file.
        if (bloomEvents != bloomRg.size() || dictEvents != dictRg.size()) {
            System.err.printf("WARN duplicate drop events: bloom %d events/%d rgs, dict %d events/%d rgs%n",
                    bloomEvents, bloomRg.size(), dictEvents, dictRg.size());
        }
        return new Result(rows, bloomRg.size(), dictRg.size(), stats, totalFromEvent);
    }

    public static void main(String[] args) throws Exception {
        String file = null, col = null, type = null, values = null;
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
            switch (args[i]) {
                case "--file" -> file = args[i + 1];
                case "--col" -> col = args[i + 1];
                case "--type" -> type = args[i + 1];
                case "--values" -> values = args[i + 1];
                default -> throw new IllegalArgumentException("unknown arg " + args[i]);
            }
        }
        if (file == null || col == null || type == null || values == null) {
            System.err.println("usage: HwBloom --file F --col C --type int|long|float|double|str|date --values v1,v2");
            System.exit(2);
        }

        Path path = Path.of(file);
        Path jfrDir = Path.of(System.getProperty("user.home"), ".cache", "zpq-bloom");
        Files.createDirectories(jfrDir);

        try (HardwoodContext ctx = HardwoodContext.create()) {
            int totalRowGroups;
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), ctx)) {
                totalRowGroups = reader.getFileMetaData().rowGroups().size();
            }
            String[] vs = values.split(",", -1);
            for (int i = 0; i < vs.length; i++) {
                Path jfrOut = jfrDir.resolve("hwbloom-" + ProcessHandle.current().pid() + "-" + i + ".jfr");
                Result r = probe(ctx, path, col, eq(col, type, vs[i]), jfrOut);
                // The statistics event carries Hardwood's own view of the row-group count; a mismatch with the
                // footer would mean the events describe some other read than the one we think we measured.
                if (r.totalFromEvent() >= 0 && r.totalFromEvent() != totalRowGroups) {
                    System.err.printf("WARN RowGroupFilter.totalRowGroups=%d but footer has %d%n",
                            r.totalFromEvent(), totalRowGroups);
                }
                System.out.printf("VALUE %s ROWS %d BLOOM_PRUNED %d DICT_PRUNED %d STATS_PRUNED %d ROW_GROUPS %d%n",
                        vs[i], r.rows(), r.bloom(), r.dict(), r.stats(), totalRowGroups);
                Files.deleteIfExists(jfrOut);
            }
        }
    }
}
