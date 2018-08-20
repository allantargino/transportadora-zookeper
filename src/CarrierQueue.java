import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.nio.ByteBuffer.allocate;

public class CarrierQueue extends SyncPrimitive.Queue{

    private String root;
    private static Integer mutex = -1;

    /**
     * Constructor of producer-consumer queue
     *
     * @param address
     * @param name
     */
    CarrierQueue(String address, String name, ZooKeeper zk) {
        super(address, name);
        this.root = name;

        createQueue(root, "priority");
        createQueue(root, "normal");
    }

    /**
     * Queue creator method
     *
     * @param path: indicates the path of data on zk server
     * @param type: "priority" for priority queue or "normal" for normal queue
     */
    private boolean createQueue(String path, String type) {
        if (type.equals("priority") || type.equals("normal")) {
            if (zk != null) {
                try {
                    Stat s = zk.exists(path, false);
                    if (s == null) {
                        zk.create(path + type + "Queue/", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                    }
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Queue adder method
     *
     * @param queueType: To insert into priority queue or normal queue
     * @param message: The message to store in
     */
    public boolean add(String queueType, String message) throws KeeperException, InterruptedException {
        if (queueType.equals("priority") || queueType.equals("normal")) {
            ByteBuffer b = null;
            b = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));

            assert b != null;
            byte[] value = b.array();
            zk.create(root + queueType + "/Queue", value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }
        return false;
    }

    /**
     * Queue getter method
     * @description: Choose where to get next item based on the priority queue. If the length of priorityQueue > 0, then retrieve from it.
     */
    public String get() throws KeeperException, InterruptedException {
        String retvalue;
        Stat stat = null;

        synchronized (mutex) {
            List<String> priorityQueue = zk.getChildren(this.root + "priorityQueue", true);
            List<String> normalQueue = zk.getChildren(this.root + "normalQueue", true);
            if (priorityQueue.size() == 0 || normalQueue.size() == 0) {
                System.out.println("Empty queue. Going to wait");
                mutex.wait();
            } else {
                List<String> list;
                String queueType;
                if (priorityQueue.size() > 0) {
                    list = priorityQueue;
                    queueType = "priorityQueue/";
                } else {
                    list = normalQueue;
                    queueType = "normalQueue/";
                }

                Integer min = new Integer(list.get(0).substring(7));
                System.out.println("List: " + list.toString());
                String minString = list.get(0);
                for (String s : list) {
                    Integer tempValue = new Integer(s.substring(7));
                    //System.out.println("Temp value: " + tempValue);
                    if (tempValue < min) {
                        min = tempValue;
                        minString = s;
                    }
               }
                byte[] b = zk.getData(this.root + "/" + queueType + minString, false, stat);
                //System.out.println("b: " + Arrays.toString(b));
                zk.delete(root + "/" + minString, 0);
                ByteBuffer buffer = ByteBuffer.wrap(b);
                retvalue = String.valueOf(buffer);
                return retvalue;
            }
        }
        return null;
    }
}
