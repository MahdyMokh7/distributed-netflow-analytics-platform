package com.nimbo.batch.support;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class SparkSessionExtension implements BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(SparkSessionExtension.class);
    private static final Object SPARK_KEY = new Object();

    private SharedSpark sharedSpark;

    @Override
    public void beforeAll(ExtensionContext context) {
        sharedSpark = context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(SPARK_KEY, ignored -> new SharedSpark(), SharedSpark.class);
    }

    public SparkSession spark() {
        if (sharedSpark == null) {
            throw new IllegalStateException("Spark extension has not started");
        }
        return sharedSpark.session;
    }

    private static final class SharedSpark implements ExtensionContext.Store.CloseableResource {
        private final SparkSession session = SparkSession.builder()
                .master("local[2]")
                .appName("batch-processor-test-suite")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        @Override
        public void close() {
            session.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
