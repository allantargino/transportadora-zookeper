import java.net.InetAddress;
import java.net.UnknownHostException;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello!");

        String localhost = getLocalHost();
        System.out.println(localhost);

        SyncPrimitive barrier = new SyncPrimitive.Barrier(localhost, "/", 10);

    }

    private static String getLocalHost() {
        String address = null;
        try {
            address = InetAddress.getLocalHost().getCanonicalHostName().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return address;
    }
}
