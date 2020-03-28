package ds.hdfs;


import com.sun.source.tree.Tree;
import netscape.javascript.JSObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.xml.crypto.Data;
import java.io.*;
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
     * Global variables
     */
    static protected Registry serverRegistry; // Might be unneeded
    static final String CONFIG_PATH = "MAPR/src/nn_config.txt"; // Path to configuration file
    static final String STORAGE_PATH = "MAPR/src/nn_files.json"; // Path to file storage
    static long blocksize = -1; // BlockSize of chunks
    int port; // The port
    String ip; // The ip address of the NameNode server
    String name; // The given name
    static HashMap<Integer, DataNode> servers = new HashMap<>(); // Stores DataNodes
    HashMap<Integer, TreeSet<String>> chunks = new HashMap<>(); // Stores the DataNode's Chunks

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
            List<String> configuration = Files.readAllLines(Paths.get(CONFIG_PATH).toAbsolutePath());

            /* Pass them to handler function to setup config state */
            HashMap<String, String> config_attr = handleConfigurationFile(configuration);

            /* Set blocksize from config */
            blocksize = Long.valueOf(config_attr.get("blocksize"));

            /* Make sure a file exists where we can persist the filenames */
            File file = new File(STORAGE_PATH);
            if(file.createNewFile()) {
                FileWriter fw = new FileWriter(STORAGE_PATH);
                fw.write("[]");
                fw.close();
                System.out.println("Created file storage...");
            }

            /* Initialize server registry for host machine (will not have to do this if `start rmiregistry`) */
            serverRegistry = LocateRegistry.createRegistry(1099);

            /* Create remote object that provides the service */
            NameNode obj = new NameNode(config_attr.get("ip"), Integer.valueOf(config_attr.get("port")), config_attr.get("name"));

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

            /* Schedule Timeout (Heartbeat Check) */
            Long interval = Long.valueOf(config_attr.get("interval"));
            Timer timer = new Timer();
            TimerTask task = new Timeout(Long.valueOf(config_attr.get("timeout")));
            timer.schedule(task, interval, interval);

            System.out.println("Will check for server status on an interval of " + interval + "ms...");

        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }


    static class Timeout extends TimerTask {

        long timeout;

        Timeout(long timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {

            /* Create iterator */
            Iterator<Map.Entry<Integer, DataNode>> server = servers.entrySet().iterator();
            while(server.hasNext()) {
                Map.Entry<Integer, DataNode> item = server.next();
                DataNode current = item.getValue();
                if (current.status) {
                    // Check timeout
                    long start = current.timestampz;

                    long end = new Date().toInstant().toEpochMilli();

                    if (end - start >= timeout) {
                        /* Change local copy */
                        current.status = false;

                        /* Update global */
                        item.setValue(current);

                        System.out.println("DataNode `" + current.id + "` has timed out.");
                    }
                }
            }
        }
    }


    private static HashMap<String, String> handleConfigurationFile(List<String> config) {
        /* Create Map */
        HashMap<String, String> result = new HashMap<>();

        for (int i = 0; i < config.size(); i += 1) {
            String line = config.get(i);
            String[] fields = line.split("=");

            if (fields.length < 2) {
                System.out.println("Configuration format should be: `<attribute>=<value>`");
                continue;
            }

            switch (fields[0]) {
                case "ip":
                    result.put("ip", fields[1]);
                    break;
                case "port":
                    result.put("port", fields[1]);
                    break;
                case "name":
                    result.put("name", fields[1]);
                    break;
                case "blocksize":
                    result.put("blocksize", fields[1]);
                    break;
                case "interval":
                    result.put("interval", fields[1]);
                    break;
                case "timeout":
                    result.put("timeout", fields[1]);
                    break;
                default:
                    System.out.println("Configuration attribute isn't recognized");
                    break;
            }
        }

        return result;
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
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] getBlockLocations(byte[] inp) throws RemoteException {
        /* Prepare response for Client */
        Proto_Defn.ReturnChunkLocations.Builder response = Proto_Defn.ReturnChunkLocations.newBuilder();

        try {
            /* Get the ClientRequest */
            Proto_Defn.ClientRequest request = Proto_Defn.ClientRequest.parseFrom(inp);

            /* Get filename, filesize, and request type from message */
            String filename = request.getFileName();
            long filesize = request.getFileSize();

            /* Retrieve and store the JSONObject else throw */
            JSONObject file_object = searchJSONFile(STORAGE_PATH, "name", filename);
            if (file_object == null) {
                throw new Error("error: given file does not exist");
            }

            response = createBlockLocationResponse(file_object);

        } catch (Exception e) {
            System.err.println("Error at getBlockLocations " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }


    private Proto_Defn.ReturnChunkLocations.Builder createBlockLocationResponse(JSONObject file) {

        Proto_Defn.ReturnChunkLocations.Builder resp = Proto_Defn.ReturnChunkLocations.newBuilder();

        TreeSet<Integer> online = getOnlineServers();
        JSONArray filelist = (JSONArray) file.get("chunks");
        for (Integer integer : online) {
            TreeSet curr = chunks.get(integer);
            for (Object chunkname : filelist) {
                Proto_Defn.ChunkLocations.Builder chunklocations = Proto_Defn.ChunkLocations.newBuilder();

                if (curr.contains(chunkname.toString())) {

                    DataNode dn = servers.get(integer);

                    Proto_Defn.DataNodeInfo.Builder dninfo = Proto_Defn.DataNodeInfo.newBuilder();

                    dninfo.setName(dn.sname);
                    dninfo.setIp(dn.ip);
                    dninfo.setPort(dn.port);
                    dninfo.setId(dn.id);

                    chunklocations.addDataNodeInfo(dninfo);
                    chunklocations.setChunkName(chunkname.toString());

                    resp.addLocations(chunklocations);

                }
            }
        }

        resp.setBlockSize((int) blocksize);
        resp.setStatus(1);

        return resp;
    }

    private TreeSet<Integer> getOnlineServers() {
        /* Instantiate TreeSet*/
        TreeSet<Integer> result = new TreeSet<>();

        /* Create iterator */
        Iterator<Map.Entry<Integer, DataNode>> server = servers.entrySet().iterator();
        while(server.hasNext()) {
            /* Get HashMap row*/
            Map.Entry<Integer, DataNode> map_curr = server.next();

            /* Get DataNode entry */
            DataNode curr = map_curr.getValue();

            /* Confirmed server is live */
            if (curr.status) {
                result.add(curr.id);
            }
        }

        return result;
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] assignBlock(byte[] inp) throws RemoteException {
        /* Prepare response for Client */
        Proto_Defn.ReturnChunkLocations.Builder response = Proto_Defn.ReturnChunkLocations.newBuilder();

        try {
            /* Get the ClientRequest */
            Proto_Defn.ClientRequest request = Proto_Defn.ClientRequest.parseFrom(inp);

            /* Get filename, filesize, and request type from message */
            String filename = request.getFileName();
            long filesize = request.getFileSize();

            /* Get chunksize from namenode_config.txt */
            long chunksize = getBlockSize();

            /* Calculate the total number of chunks to create */
            long number_of_chunks = calculateChunkAmount(filesize, chunksize);

            /* Create the chunk names that will be distributed to DataNodes */
            // TreeSet<String> dn_chunk_names = new TreeSet<>(); ???
            JSONArray dn_chunk_names = new JSONArray();
            for (int i = 0; i < number_of_chunks; i += 1) {
                dn_chunk_names.add(filename + ":" + chunksize + ":" + i + ":" + new Date().toInstant().toEpochMilli());
            }

            /* Create JSON object to write to file */
            JSONObject json = new JSONObject();

            /* Add file headers */
            json.put("name", filename);
            json.put("size", filesize);
            json.put("handle", 0);

            /* Add chunks to JSON object */
            json.put("chunks", dn_chunk_names);

            /* Create array to input into file */
            JSONArray tmp = new JSONArray();
            tmp.add(json);

            /* Append JSON to storage file */
            appendJSONFile(STORAGE_PATH, tmp);

            /* Setup reponse */
            response.setBlockSize((int)blocksize);

            /* Add file chunks to response */
            Iterator<String> item = dn_chunk_names.iterator();
            while (item.hasNext()) {
                Proto_Defn.ChunkLocations.Builder chunk = Proto_Defn.ChunkLocations.newBuilder();
                chunk.setChunkName(item.next());
                response.addLocations(chunk);
            }

        } catch (Exception e) {
            System.err.println("Error at AssignBlock " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    private JSONObject searchJSONFile(String path, String attr, String value) {
        try {
            /* Read storage file */
            JSONArray storage = readJSONFile(path);

            /* Iterate through files, check if given exists*/
            Iterator<?> item = storage.iterator();
            while (item.hasNext()) {
                /* Get json from ArrayList */
                JSONObject current = (JSONObject) item.next();

                /* Check if provided a valid attribute */
                Object attribute = current.get(attr);
                if (attribute == null) {
                    throw new Error("error: please enter a valid attribute");
                }

                /* Check if attribute matches given value */
                if (attribute.equals(value)) {
                    return current;
                }
            }
        } catch (Exception e) {
            System.err.println("Error at searchJSONFile " + e.toString());
            e.printStackTrace();
        }

        return null;
    }

    private void appendJSONFile(String path, JSONArray arr) throws  FileNotFoundException {
        try {
            /* Read storage file and place in a JSON array */
            JSONArray old = readJSONFile(path);

            /* Write new concat JSON array to file */
            writeJSONFile(path, concatArray(old, arr));

        } catch (Exception e) {
            System.err.println("Error at appendJSONFile " + e.toString());
            e.printStackTrace();
        }

        return;
    }

    private void writeJSONFile(String path, JSONArray arr) throws  FileNotFoundException {
        try {
            /* Open storage file */
            FileWriter fw = new FileWriter(path);

            /* Place JSON in file */
            fw.write(arr.toJSONString());

            /* Close to save the write */
            fw.close();
        } catch (Exception e) {
            System.err.println("Error at writeJSONFile " + e.toString());
            e.printStackTrace();
        }

        return;
    }

    // REMOVE STATIC LATER
    private JSONArray readJSONFile(String path) throws FileNotFoundException {
        JSONArray json = new JSONArray();
        try {
            /* Instantiate the parser */
            JSONParser parser = new JSONParser();

            /* Parse and return the storage file */
            json = (JSONArray)parser.parse(new FileReader(path));

        } catch (Exception e) {
            System.err.println("Error at readJSONFile " + e.toString());
            e.printStackTrace();
        }

        return json;
    }

    private JSONArray concatArray(JSONArray arr1, JSONArray arr2) {
        JSONArray result = new JSONArray();
        for (int i = 0; i < arr1.size(); i++) {
            result.add(arr1.get(i));
        }
        for (int i = 0; i < arr2.size(); i++) {
            result.add(arr2.get(i));
        }

        return result;
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] list(byte[] inp) throws RemoteException {
        Proto_Defn.ListResult.Builder response = Proto_Defn.ListResult.newBuilder();

        try {
            JSONArray file = readJSONFile(STORAGE_PATH);
            Iterator<?> item = file.iterator();
            while (item.hasNext()) {
                JSONObject current = (JSONObject) item.next();

                String filename = current.get("name").toString();
                response.addFileName(filename);
            }
        } catch (Exception e) {
            System.err.println("Error at list " + e.toString());
            e.printStackTrace();
//            response.setStatus(-1);
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
        Proto_Defn.WriteBlockResponse.Builder response = Proto_Defn.WriteBlockResponse.newBuilder();

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
            response.setStatus(false);
        }

        return response.build().toByteArray();
    }

    /**
     *
     */
    public static class DataNode {
        String ip;
        String sname;
        long timestampz;
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
            this.timestampz = new Date().toInstant().toEpochMilli();
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
