package ds.hdfs;

import ds.hdfs.hdfsformat.*;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;

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


        // Grab IP Address + Port

        int port = 10;
        String addr = "127";


        /* Create an instance of the NameNode server */
        NameNode server = new NameNode("1asdads23", 1, "asd");

        /* Register the instance with the name server (rmiregisty) */
        Naming.rebind("NameNode", server);

        System.out.println("Server is running on port: " + port);
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
