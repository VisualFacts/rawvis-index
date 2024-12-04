package gr.athenarc.imsi.visualfacts.experiments.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

public class SyntheticDatasetGenerator {

    private static final Logger LOG = LogManager.getLogger(SyntheticDatasetGenerator.class);

    private int rowCount;
    private int colCount;
    private int cardinality;
    private List<Integer> categoricalCols;
    private String file;
    private RandomDataGenerator randomDataGenerator;

    public SyntheticDatasetGenerator(int rowCount, int colCount, List<Integer> categoricalCols, int cardinality, String file) {
        this.rowCount = rowCount;
        this.colCount = colCount;
        this.categoricalCols = categoricalCols;
        this.cardinality = cardinality;
        this.file = file;
        randomDataGenerator = new RandomDataGenerator();
        randomDataGenerator.reSeed(0);
    }


    public void generate() throws IOException {
        double generatedDouble = randomDataGenerator.nextUniform(0d, 1000d);
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(file), csvWriterSettings);
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < colCount; col++) {
                csvWriter.addValue(categoricalCols.contains(col) ? randomDataGenerator.nextInt(0, cardinality - 1) + "000000000" :
                        String.format(Locale.ROOT, "%.6f", randomDataGenerator.nextUniform(0d, 1000d)));
            }
            csvWriter.writeValuesToRow();
        }
        csvWriter.close();
    }

public static void main(String[] args) throws IOException {
        int rowCount = 1000000; // 1 million rows
        int colCount = 10; // Number of columns 
        int cardinality = 0; // Not used since we have no categorical columns
        List<Integer> categoricalCols = Collections.emptyList(); // Empty list for categorical columns
        String file = "synth1M_10cols.csv"; // Output file

        SyntheticDatasetGenerator generator = new SyntheticDatasetGenerator(rowCount, colCount, categoricalCols, cardinality, file);
        generator.generate();
    }


}
