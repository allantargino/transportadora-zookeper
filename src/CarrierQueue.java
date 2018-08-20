import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

public class CarrierQueue {

    Queue priorityQueue;
    Queue normalQueue;

    public CarrierQueue() {
        priorityQueue = new PriorityQueue();
        normalQueue = new SynchronousQueue();
    }

    public void addToPriorityQueue(String packageDescription) {
        this.priorityQueue.add(packageDescription);
    }

    public void addToNormalQueue(String packageDescription) {
        this.normalQueue.add(packageDescription);
    }

    public String getFromPriorityQueue() {
        return String.valueOf(this.priorityQueue.poll());
    }

    public String getFromNormalQueue() {
        return String.valueOf(this.normalQueue.poll());
    }
}
