package nes.calcite;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CyclicMetadataException;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.DelegatingMetadataRel;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Calcite's metadata handlers without runtime code generation, so they work in a native image.
 *
 * Calcite's default handlers are classes it generates and compiles with Janino at runtime, which a native image cannot
 * load. ProxyingMetadataHandlerProvider dispatches through java.lang.reflect.Proxy instead, but unlike the generated
 * handlers it does not cache: join reordering then recomputes e.g. the row counts of the whole join tree for every
 * candidate, and a 60-table join took 40 s instead of 5 s. This adds the generated handlers' cache: per
 * RelMetadataQuery (RelMetadataQuery.map), keyed by node, method and arguments, with the same cycle detection, and
 * their unwrapping of DelegatingMetadataRel (e.g. the planner's HepRelVertex), for which the proxies return nothing.
 */
final class CachingMetadataHandlerProvider implements MetadataHandlerProvider {
    static final CachingMetadataHandlerProvider INSTANCE = new CachingMetadataHandlerProvider();

    private final MetadataHandlerProvider uncached = new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
    private final Map<Class<?>, Object> handlers = new ConcurrentHashMap<>();

    private CachingMetadataHandlerProvider() {}

    @Override
    public <MH extends MetadataHandler<?>> MH handler(Class<MH> handlerClass) {
        return handlerClass.cast(handlers.computeIfAbsent(handlerClass, c -> cachingHandler(handlerClass)));
    }

    private <MH extends MetadataHandler<?>> Object cachingHandler(Class<MH> handlerClass) {
        MH handler = uncached.handler(handlerClass);
        return Proxy.newProxyInstance(handlerClass.getClassLoader(), new Class<?>[] {handlerClass}, (proxy, method, args) -> {
            if (args == null || args.length < 2 || !(args[0] instanceof RelNode node) || !(args[1] instanceof RelMetadataQuery mq)) {
                return invoke(handler, method, args); // e.g. getDef()
            }
            /// E.g. a HepRelVertex answers for the node it holds, as with the generated handlers.
            RelNode rel = node;
            while (rel instanceof DelegatingMetadataRel delegating && delegating.getMetadataDelegateRel() != rel) {
                rel = delegating.getMetadataDelegateRel();
            }
            if (rel != node) {
                args = args.clone();
                args[0] = rel;
            }
            List<Object> key = new ArrayList<>(args.length - 1);
            key.add(method);
            key.addAll(Arrays.asList(args).subList(2, args.length));
            Object cached = mq.map.get(rel, key);
            if (cached == NullSentinel.ACTIVE) {
                throw new CyclicMetadataException();
            }
            if (cached != null) {
                return cached == NullSentinel.INSTANCE ? null : cached;
            }
            mq.map.put(rel, key, NullSentinel.ACTIVE);
            try {
                Object result = invoke(handler, method, args);
                mq.map.put(rel, key, result == null ? NullSentinel.INSTANCE : result);
                return result;
            } catch (Throwable e) {
                mq.map.remove(rel, key);
                throw e;
            }
        });
    }

    private static Object invoke(Object handler, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(handler, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
