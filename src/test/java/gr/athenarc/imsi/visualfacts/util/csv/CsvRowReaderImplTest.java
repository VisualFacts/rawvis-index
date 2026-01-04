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

class CsvRowReaderImplTest {
    private static final Logger LOG = LogManager.getLogger(CsvRowReaderImplTest.class);

    @Test
    void customReaderMatchesUnivocity() throws IOException {
        Path csvPath = Paths.get("src/test/resources/data/data_10_cols_10M.csv");

        CsvReaderConfig config = new CsvReaderConfig(
                csvPath.toFile(),
                StandardCharsets.UTF_8,
                null,
                false,
                ',');

        try (CsvRowReader customRowReader = new CsvRowReaderImpl();
                CsvRowReader uni = new UnivocityCsvRowReader()) {
            customRowReader.open(config);
            uni.open(config);

            String[] customRow;
            String[] uniRow;
            long customOffset;
            long uniOffset;
            int rowCount = 0;

            while ((customRow = customRowReader.nextRow()) != null) {
                uniRow = uni.nextRow();
                customOffset = customRowReader.currentOffset();
                uniOffset = uni.currentOffset();
                assertArrayEquals(uniRow, customRow, "Row mismatch at index " + rowCount);
                assertEquals(uniOffset, customOffset, "Offset mismatch at index " + rowCount);
                if (rowCount % 1000000 == 0) {
                    LOG.info("Row " + rowCount + ": " + Arrays.toString(customRow) +
                            " offset=" + customOffset + " (uni offset=" + uniOffset + ")");
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
