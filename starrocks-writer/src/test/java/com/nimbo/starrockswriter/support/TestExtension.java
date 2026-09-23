package com.nimbo.starrockswriter.support;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.lifecycle.Startables;

public final class TestExtension implements BeforeAllCallback, ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(TestExtension.class);
    private static final Infrastructure INFRASTRUCTURE = new Infrastructure();

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot().getStore(NAMESPACE).getOrComputeIfAbsent(
                Infrastructure.class,
                ignored -> {
                    INFRASTRUCTURE.start();
                    return INFRASTRUCTURE;
                },
                Infrastructure.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == TestExtension.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return this;
    }

    public TestKafka kafka() {
        return INFRASTRUCTURE.kafka();
    }

    public TestStarRocks starRocks() {
        return INFRASTRUCTURE.starRocks();
    }

    static Infrastructure infrastructure() {
        return INFRASTRUCTURE;
    }

    static final class Infrastructure implements ExtensionContext.Store.CloseableResource {

        private final TestKafka kafka = new TestKafka();
        private final TestStarRocks starRocks = new TestStarRocks();
        private boolean started;

        synchronized void start() {
            if (started) {
                return;
            }

            try {
                Startables.deepStart(kafka.container(), starRocks.container()).join();
                started = true;
            } catch (RuntimeException exception) {
                close();
                throw exception;
            }
        }

        synchronized void prepare() {
            start();
            kafka.reset();
            starRocks.resetSchema();
        }

        TestKafka kafka() {
            return kafka;
        }

        TestStarRocks starRocks() {
            return starRocks;
        }

        @Override
        public synchronized void close() {
            stop(kafka.container());
            stop(starRocks.container());
            started = false;
        }

        private void stop(org.testcontainers.containers.GenericContainer<?> container) {
            if (container.isRunning()) {
                container.stop();
            }
        }
    }
}
