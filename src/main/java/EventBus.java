import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

public class EventBus {
    private Map<String, List<Event>> topics;
    // S1 -> map (T1 -> offset) (T2 -> offset2)
    // S2 -> map (T2 -> offsetX)
    private Map<String, Map<String, Long>> subscriberMetaData;
    private WriteExecutor writeExecutor;
    private ReadExecutor readExecutor;

    public EventBus() {
        topics = new ConcurrentHashMap<>();
        subscriberMetaData = new ConcurrentHashMap<>();
        writeExecutor = new WriteExecutor(10);
        readExecutor = new ReadExecutor(10);
    }

    // Publish will run asynchronously
    public void publish(String topic, Event event) {
        writeExecutor.submitTask(topic, () -> topics.get(topic).add(event));
    }

    // Subscriber S1 registers to start reading from topic T1 with a offset
    // Remember where you last left after reading from the topic
    public void subscribe(String topic, String subscriberId) {
        Map<String, Long> topicOffset = subscriberMetaData.get(subscriberId);
        if (topicOffset == null) {
            topicOffset.put(topic, 1L);
        } else {
            // Already subscribed
        }
    }

    // Subscriber S1, needs to follow order in which the events are published
    // While reading only one thread should read what is the content for subscriber + topic pair
    public void read(String topic, String subscriberId) {
        // TODO:
        subscriberMetaData.get(subscriberId).get(topic);
    }
}

class WriteExecutor {
    private Executor[] executors;

    public WriteExecutor(int threads) {
        for (int count = 0; count < threads; count++) {
            executors[count] = Executors.newSingleThreadExecutor();
        }
    }

    // To make sure that the order is maintained, take a hash of the topic and modulo by Total threads
    // so all the requests on 1 topic will always be taken care by one thread.
    // The task is just to add elements to the topic
    public <T> CompletableFuture<T> submitTask(String topic, Supplier<T> task) {
        return CompletableFuture.supplyAsync(task, executors[topic.hashCode() % executors.length]);
    }
}

class ReadExecutor {
    private Executor[] executors;

    public ReadExecutor(int thread) {
        for (int count = 0; count < thread; count++) {
            executors[count] = Executors.newSingleThreadExecutor();
        }
    }

    public <T> CompletableFuture<T> readEvents(String topic, String subscriberId, Map<String, List<Event>> topics) {
        // TODO:
        return null;
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

// TODO: Complete later
class ExponentialBackOff<P, R> extends RetryAlgorithm<P, R> {

    @Override
    public void retry(Function<P, R> task, P param) {
        try {
            task.apply(param);
        } catch (Exception exception) {

        }
    }
}



