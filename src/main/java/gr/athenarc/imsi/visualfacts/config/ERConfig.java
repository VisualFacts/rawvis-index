package gr.athenarc.imsi.visualfacts.config;

import org.apache.commons.configuration.ConfigurationRuntimeException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.HashSet;

public final class ERConfig {
    private static HashSet<String> stopwords;
 

	public static HashSet<String> getStopwords() {
        try {
            if (stopwords == null) {
                ObjectInput input = new ObjectInputStream(new BufferedInputStream(ERConfig.class.getClassLoader().getResourceAsStream("stopwords_SER")));
                stopwords = (HashSet<String>) input.readObject();
            }
            return stopwords;
        } catch (IOException | ClassNotFoundException e) {
            throw new ConfigurationRuntimeException(e);
        }
    }


}