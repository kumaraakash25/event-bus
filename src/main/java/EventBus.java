import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class EventBus {
    private Map<String, List<Event>> topics;
    private Map<String, Set<String>> pullSubscribers;
    private Map<String, Set<String>> pushSubscribers;
    KeyedExecutor executor;

    public EventBus() {
        topics = new ConcurrentHashMap<>();
        pullSubscribers = new ConcurrentHashMap<>();
        executor = new KeyedExecutor(10);
    }

    public void publish(String topic, Event event) {
        executor.submit(topic, () -> topics.get(topic).add(event));
        topics.get(topic).add(event);
    }

    public void subscribePull(String topic, String subscriber) {
        pullSubscribers.get(topic).add(subscriber);
    }

    public void subscribePush(String topic, String subscriber) {
        pushSubscribers.get(topic).add(subscriber);
    }

}

class KeyedExecutor {
    private Executor[] executors;

    public KeyedExecutor(int threads) {
        for (int count = 0; count< threads; count++) {
            executors[count] = Executors.newSingleThreadExecutor();
        }
    }


    public CompletionStage<Void> submit(String id, Runnable task) {
        return CompletableFuture.runAsync(task, executors[id.hashCode() % executors.length]);
    }


    public <T> CompletionStage<T> get(String id, Supplier<T> task) {
        return CompletableFuture.supplyAsync(task, executors[id.hashCode() % executors.length]);
    }
}

class Event {
    private String id;
    private String name;
    private Map<String, Object> fields;

    public Event(String id, String name, Map<String, Object> fields) {
        this.id = id;
        this.name = name;
        this.fields = fields;
    }

}

abstract class RetryAlgorithm<P, R> {
    public abstract void retry(Function<P, R> task, P param);
}

// TODO: Will do later
class ExponentialBackOff<P, R> extends RetryAlgorithm<P, R> {

    @Override
    public void retry(Function<P, R> task, P  param) {
        try {
            task.apply(param);
        } catch (Exception exception) {

        }
    }
}



