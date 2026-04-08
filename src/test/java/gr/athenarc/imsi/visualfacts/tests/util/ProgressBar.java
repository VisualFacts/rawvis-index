package gr.athenarc.imsi.visualfacts.tests.util;

/**
 * Simple inline progress bar for test output.
 */
public final class ProgressBar {

    private static final int BAR_WIDTH = 40;

    private ProgressBar() {
    }

    /**
     * Prints an inline progress bar to stderr (overwrites the current line).
     * Call with current = total for the last update, then call {@link #finish()} to move to the next line.
     */
    public static void print(String label, int current, int total) {
        int filled = (int) ((double) current / total * BAR_WIDTH);
        StringBuilder sb = new StringBuilder("\r");
        sb.append(label).append(": [");
        for (int i = 0; i < BAR_WIDTH; i++) {
            sb.append(i < filled ? '=' : ' ');
        }
        sb.append("] ").append(current).append('/').append(total);
        System.err.print(sb);
    }

    /**
     * Prints a newline to stderr after the progress bar is complete.
     */
    public static void finish() {
        System.err.println();
    }
}
