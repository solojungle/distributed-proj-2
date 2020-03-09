package ds.hdfs;

import ds.hdfs.hdfsformat.*;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

/**
 * Creates an instance of the remote object implementation,
 * Exports the remote object,
 * and then binds that instance to a name in a Java RMI registry.
 */
public class NameNode implements INameNode {

    /**
     *
     */
    protected Registry serverRegistry;

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

            System.out.println("Server is running...");
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
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] openFile(byte[] inp) throws RemoteException {
        ResponseBuilder response = Response.ok();
        try {
        } catch (Exception e) {
            System.err.println("Error at " + getClass() + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }
        return response.toByteArray();
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] closeFile(byte[] inp) throws RemoteException {
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
        try {
        } catch (Exception e) {
            System.err.println("Error at list " + e.toString());
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
    public byte[] blockReport(byte[] inp) throws RemoteException {
        try {
        } catch (Exception e) {
            System.err.println("Error at blockReport " + e.toString());
            e.printStackTrace();
            response.addStatus(-1);
        }
        return response.build().toByteArray();
    }

    /**
     * @param inp
     * @return
     * @throws RemoteException
     */
    public byte[] heartBeat(byte[] inp) throws RemoteException {
        return response.build().toByteArray();
    }

    /**
     * @param msg
     */
    public void printMsg(String msg) {
        System.out.println(msg);
    }

    /**
     *
     */
    public static class DataNode {
        String ip;
        int port;
        String serverName;

        /**
         * @param addr
         * @param p
         * @param sname
         */
        public DataNode(String addr, int p, String sname) {
            ip = addr;
            port = p;
            serverName = sname;
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
