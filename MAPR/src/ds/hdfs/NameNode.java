package ds.hdfs;


import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * Creates an instance of the remote object implementation,
 * Exports the remote object,
 * and then binds that instance to a name in a Java RMI registry.
 */
public class NameNode implements INameNode {

    /**
     *
     */

    String ip;
    String name;
    int port;
    HashMap<Integer, DataNode> servers = new HashMap<>();


    static protected Registry serverRegistry;

    /**
     * @param addr
     * @param p
     * @param nn
     */
    public NameNode(String addr, int p, String nn) {
        ip = addr;
        port = p;
        name = nn;
    }

    /**
     * Entry point to instantiate the NameNode server
     *
     * @param args
     * @throws InterruptedException
     * @throws NumberFormatException
     * @throws IOException
     */
    public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException {
        try {

            serverRegistry = LocateRegistry.createRegistry(1099);

            /* Create remote object that provides the service */
            NameNode obj = new NameNode("localhost", 3000, "nn");

            /* Remote object exported to the Java RMI runtime so that it may receive incoming remote calls */
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

            /*
             * Returns a stub that implements the remote interface java.rmi.registry.
             * Sends invocations to the registry on server's local host on the default registry port of 1099.
             * */
            Registry registry = LocateRegistry.getRegistry();
//            Registry registry = LocateRegistry.createRegistry(1099);

            /* Bind the remote object's stub in the registry. */
            registry.bind("NameNode", stub);

            System.out.println("NameNode server is running...");


            System.out.println("Will check for server status on an interval of 3 secs...");

//            System.out.println(Arrays.toString(registry.list()));

        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Searches the file-list for a file
     *
     * @param fhandle
     * @return bool
     */
    boolean findInFilelist(int fhandle) {
        return false;
    }

    /**
     * Prints the entirety of file-list
     */
    public void printFilelist() {

    }

    /**
     * Method to open a file given file name with read-write flag
     *
     * @param inp - A byte array of a .proto ClientRequest
     * @return
     * @throws RemoteException
     */
    public byte[] openFile(byte[] inp) throws RemoteException {
//        Proto_Defn.ClientRequest input
        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
        } catch (Exception e) {
            System.err.println("Error at " + getClass() + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }
        return response.build().toByteArray();
    }

    /**
     * @param inp - A byte array of a .proto ClientRequest
     * @return
     * @throws RemoteException
     */
    public byte[] closeFile(byte[] inp) throws RemoteException {

        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
        } catch (Exception e) {
            System.err.println("Error at closefileRequest " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] getBlockLocations(byte[] inp) throws RemoteException {

        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
        } catch (Exception e) {
            System.err.println("Error at getBlockLocations " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }
        return response.build().toByteArray();
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] assignBlock(byte[] inp) throws RemoteException {

        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
        } catch (Exception e) {
            System.err.println("Error at AssignBlock " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] list(byte[] inp) throws RemoteException {

        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {

            System.out.println(new String(inp));

        } catch (Exception e) {
            System.err.println("Error at list " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }
        return response.build().toByteArray();
    }


    /**
     * Handles the insertion of DataNodes, will "refresh" the timestampz and status if server already exists
     *
     * @param n - DataNode to be inserted into HashMap
     */
    private void handleBlockReportInsert(DataNode n) {
        /* DataNode already exists, overwrite it's status and timestamp */
        if (servers.containsKey(n.id)) {
            DataNode temp = new DataNode(n.ip, n.port, n.sname, n.id);
            servers.replace(temp.id, temp);
            return;
        }

        /* New DataNode has made contact, insert into HashMap of DataNodes */
        servers.put(n.id, n);
        return;
    }

    /**
     * Used to send a BlockReport from a DataNode to the NameNode, populates DataNode HashMap.
     *
     * @param inp - A byte array of a .proto BlockReport
     * @return
     * @throws RemoteException
     */
    public byte[] blockReport(byte[] inp) throws RemoteException {
        /* Prepare the response to Client */
        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
            /* Get the BlockReport */
            Proto_Defn.BlockReport b = Proto_Defn.BlockReport.parseFrom(inp);

            /* Pull DataNodeInfo from message */
            Proto_Defn.DataNodeInfo info = b.getDataNodeInfo();
            String address = info.getIp();
            String name = info.getName();
            int id = info.getId();
            int port = info.getPort();

            /* Create DataNode instance to insert into list */
            DataNode temp = new DataNode(address, port, name, id);

            /* Insert/refresh DataNode in HashMap */
            handleBlockReportInsert(temp);

//            b.setDataNodeInfo(d);
//            for(String f: MyChunks) {
//                b.addChunkName(f);
//            }
        } catch (Exception e) {
            System.err.println("Error at blockReport " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    /**
     *
     */
    public static class DataNode {
        Date timestampz;
        String ip;
        String sname;
        boolean status;
        int port;
        int id;

        /**
         * @param ip
         * @param port
         * @param sname
         * @param id
         */
        public DataNode(String ip, int port, String sname, int id) {
            this.ip = ip;
            this.port = port;
            this.sname = sname;
            this.id = id;
            this.timestampz = new Date();
            this.status = true;
        }
    }

    /**
     *
     */
    public static class FileInfo {
        String filename;
        int filehandle;
        boolean writemode;
        ArrayList<Integer> Chunks;

        /**
         * @param name
         * @param handle
         * @param option
         */
        public FileInfo(String name, int handle, boolean option) {
            filename = name;
            filehandle = handle;
            writemode = option;
            Chunks = new ArrayList<Integer>();
        }
    }

}
