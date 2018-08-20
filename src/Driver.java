public class Driver {

    public static void main(String[] args) {
        try {
            String hostPort  = args[0];

            //TODO: Use SyncPrimitive to create zk client

            registerAsAvailableDriver();

            while(true){
                if(existShipments()){
                    String content = getMessageToDeliver();
                    deliverMessage(content);
                }else{
                    Thread.sleep(5000);
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void registerAsAvailableDriver(){
        System.out.println("Entering the place...");
        //TODO: Connect to zk and register as an available node
    }

    private static void unregisterAsAvailableDriver(){
        System.out.println("Leaving the place...");
        //TODO: Connect to zk and delete its node
    }

    private static boolean existShipments(){
        //TODO: Check if queue is empty
        return true;
    }

    private static String getMessageToDeliver(){
        //TODO: Consume queue
        return "a message";
    }

    private static void deliverMessage(String message) throws InterruptedException{
        unregisterAsAvailableDriver();
        System.out.println("Start Delivering message: " + message);
        Thread.sleep(10000);
        System.out.println("Message delivered.");
        registerAsAvailableDriver();
    }

}
