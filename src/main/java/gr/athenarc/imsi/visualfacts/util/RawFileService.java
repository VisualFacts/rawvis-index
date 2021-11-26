package gr.athenarc.imsi.visualfacts.util;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.athenarc.imsi.visualfacts.Schema;
import gr.athenarc.imsi.visualfacts.util.io.RandomAccessReader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RawFileService {

    private RandomAccessReader randomAccessReader;

    //todo cache objects read from file
    private Map<Long, String[]> cache = new HashMap<>();

    private CsvParser parser;

    public RawFileService(Schema schema) throws IOException {
        this.randomAccessReader = RandomAccessReader.open(new File(schema.getCsv()));
        CsvParserSettings parserSettings = schema.createCsvParserSettings();
        parser = new CsvParser(parserSettings);
    }

    public String[] getObject(long offset) throws IOException {
        String[] object;
        if (!cache.containsKey(offset)) {
            randomAccessReader.seek(offset);
            String row = randomAccessReader.readLine();
            if (row != null) {
                try {
                    object = parser.parseLine(row);
                }
                catch(Exception e) {object = null;}
            } else object = null;
            cache.put(offset, object);
        } else {
            object = cache.get(offset);
        }
        return object;
    }
}
