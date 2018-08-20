import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.nio.ByteBuffer.allocate;

public class CarrierQueue extends SyncPrimitive.Queue{

    String root;

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
     * Constructor of producer-consumer queue
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
}
