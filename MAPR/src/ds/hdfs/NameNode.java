package ds.hdfs;


import com.sun.source.tree.Tree;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Creates an instance of the remote object implementation,
 * Exports the remote object,
 * and then binds that instance to a name in a Java RMI registry.
 */
public class NameNode implements INameNode {

    /**
     *
     */

    String ip; // The ip address of the NameNode server
    String name; // The given name
    int port; // The port
    static long blocksize = -1; // BlockSize of chunks
    HashMap<Integer, DataNode> servers = new HashMap<>(); // Stores DataNodes
    HashMap<Integer, TreeSet<String>> chunks = new HashMap<>(); // Stores the DataNode's Chunks


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

            /* Get all new-lines from the configuration file */
            List<String> configuration = Files.readAllLines(Paths.get("MAPR/src/nn_config.txt").toAbsolutePath());
            /* Pass them to handler function to setup config state */
            handleConfigurationFile(configuration);

            /* Initialize server registry for host machine (will not have to do this if `start rmiregistry`) */
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

            /* Bind the remote object's stub in the registry. */
            registry.bind("NameNode", stub);

            System.out.println("NameNode server is running...");

            // Start timer
//            System.out.println("Will check for server status on an interval of 3 secs...");

        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }


    private static void handleConfigurationFile(List<String> config) {

        for (int i = 0; i < config.size(); i += 1) {
            String line = config.get(i);
            String[] fields = line.split("=");

            if (fields.length < 2) {
                System.out.println("Configuration format should be: `<attribute>=<value>`");
                continue;
            }

            switch (fields[0]) {
                case "ip":
                    break;
                case "port":
                    break;
                case "blocksize":
                    blocksize = Long.valueOf(fields[1]);
                    break;
                default: System.out.println("Configuration attribute isn't recognized");
            }
        }
    }

    private long getBlockSize() throws RemoteException {
        if (blocksize < 1) {
            throw new RemoteException("Blocksize variable was not set correctly in NameNode configuration");
        }

        return blocksize;
    }

    private long calculateChunkAmount(long filesize, long blocksize) {
        /* Chunks can only be whole numbers not fractions, so we need to round up */
        long size = (long)Math.ceil((double)filesize / blocksize);

        /* If file is empty create a single chunk */
        if (size == 0) {
            size = 1;
        }

        return size;
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
        /* Prepare response for Client */
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
        /* Prepare response for Client */
        Proto_Defn.Response.Builder response = Proto_Defn.Response.newBuilder();

        try {
            /* Get the ClientRequest */
            Proto_Defn.ClientRequest request = Proto_Defn.ClientRequest.parseFrom(inp);

            /* Get filename, filesize, and request type from message */
            Proto_Defn.ClientRequest.ClientRequestType type = request.getRequestType();
            String filename = request.getFileName();
            long filesize = request.getFileSize();

            /* Get chunksize from nn_config.txt */
            long chunksize = getBlockSize();

            /* Calculate the total number of chunks to create */
            long number_of_chunks = calculateChunkAmount(filesize, chunksize);

            /* Create the chunk names that will be distributed to DataNodes */
            TreeSet<String> dn_chunk_names = new TreeSet<>();
            for (int i = 0; i < number_of_chunks; i += 1) {
                dn_chunk_names.add(filename + ":" + chunksize + ":" + i + ":" + new Date());
            }

            File file = new File("nn_files.json");


            // Create .json file of all chunks of files


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
        if (this.servers.containsKey(n.id)) {
            DataNode temp = new DataNode(n.ip, n.port, n.sname, n.id);
            servers.replace(temp.id, temp);
            return;
        }

        /* New DataNode has made contact, insert into HashMap of DataNodes */
        servers.put(n.id, n);
        return;
    }

    /**
     *
     * @param id
     * @param chunks
     */
    private void handleChunkInsert(int id, TreeSet<String> chunks) {
        /* Checking GLOBAL chunks HashMap for the existence of DataNode id */
        if (this.chunks.containsKey(id)) {
            /* Replace old chunks with new */
            this.chunks.replace(id, chunks);
            return;
        }

        /* Insert new chunks into HashMap */
        this.chunks.put(id, chunks);
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
            Proto_Defn.BlockReport request = Proto_Defn.BlockReport.parseFrom(inp);

            /* Pull DataNodeInfo from message */
            Proto_Defn.DataNodeInfo info = request.getDataNodeInfo();
            String address = info.getIp();
            String name = info.getName();
            int id = info.getId();
            int port = info.getPort();

            /* Create DataNode instance to insert into list */
            DataNode temp = new DataNode(address, port, name, id);

            /* Insert new DataNode into HashMap, or recreate DataNode with a new timestampz */
            handleBlockReportInsert(temp);

            /* Instantiate TreeSet to hold chunks */
            TreeSet<String> dn_chunks = new TreeSet<>();

            /* Loop through chunks in BlockReport */
            for (String chunk : request.getChunkNameList() ) {
                dn_chunks.add(chunk);
            }

            /* Insert DataNode chunks into the HashMap */
            handleChunkInsert(temp.id, dn_chunks);

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
