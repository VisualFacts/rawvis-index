package gr.athenarc.imsi.visualfacts.util.csv;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class ZsvCsvDoubleRowReaderTest {
    private static final Logger LOG = LogManager.getLogger(ZsvCsvDoubleRowReaderTest.class);

    @Test
    void zsvReaderMatchesUnivocity() throws IOException {
        Path csvPath = Paths.get("src/test/resources/data/data_10_cols_100K.csv");
        // Path csvPath = Paths.get("/data-nonraid/maroulis/data/taxi_data/yellow_tripdata_2014_cleaned_10M.csv");
        // Path csvPath = Paths.get("/home/stavmars/data/sdss_136k.csv");
        // Path csvPath = Paths.get("/home/stavmars/data/test.csv");

        CsvReaderConfig config = new CsvReaderConfig(
                csvPath.toFile(),
                StandardCharsets.UTF_8,
                new int[] { 0, 1, 5, 6},
                false,
                ',');

        try (CsvDoubleRowReader zsvReader = new ZsvCsvDoubleRowReader();
                CsvDoubleRowReader uni = new UnivocityCsvDoubleRowReader()) {
            zsvReader.open(config);
            uni.open(config);

            double[] zsvRow;
            double[] uniRow;
            long zsvOffset;
            long uniOffset;
            int rowCount = 0;

            while ((zsvRow = zsvReader.nextRow()) != null) {
                uniRow = uni.nextRow();
                zsvOffset = zsvReader.currentOffset();
                uniOffset = uni.currentOffset();
                assertArrayEquals(uniRow, zsvRow, 1e-10, "Row mismatch at index " + rowCount);
                assertEquals(uniOffset, zsvOffset, "Offset mismatch at index " + rowCount);
                if (rowCount % 1000 == 0) {
                    LOG.trace("Row " + rowCount + ": " + Arrays.toString(zsvRow) +
                            " offset=" + zsvOffset + " (uni offset=" + uniOffset + ")");
                }
                rowCount++;
            }
            // ensure both readers reached EOF simultaneously
            assertEquals(null, uni.nextRow());
            assertTrue(rowCount > 0, "CSV file should contain rows");
            LOG.info("Total rows compared: " + rowCount);
        }
    }
}
