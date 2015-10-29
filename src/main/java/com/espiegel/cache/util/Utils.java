package com.espiegel.cache.util;

public final class Utils {
	
	public static Runnable unchecked(CheckedRunnable t) {
        return () -> {
            try {
                t.run();
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

	@FunctionalInterface
	public interface CheckedRunnable {
		void run() throws Exception;
	}
	
	// Utility class, don't instantiate.
	private Utils() {}
}
