//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import ds.hdfs.IDataNode.*;
import ds.hdfs.Proto_Defn.BlockReport;
import ds.hdfs.Proto_Defn.DataNodeInfo;
import ds.hdfs.Proto_Defn.ReadBlockRequest;
import ds.hdfs.Proto_Defn.ReadBlockResponse;
import ds.hdfs.Proto_Defn.WriteBlockRequest;
import ds.hdfs.Proto_Defn.WriteBlockResponse;;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int MyID;

    private ArrayList<String> MyChunks;
    
    public DataNode()
    {
    	//load chunks into MyChunks arr
    }
    
    

    
    //IGNORE FOR THIS PART OF PROJECT
    /*
    public static void appendtoFile(String Filename, String Line)
    {
        BufferedWriter bw = null;

        try {
            //append
        } 
        catch (IOException ioe) 
        {
            ioe.printStackTrace();
        } 
        finally 
        {                       // always close the file
            if (bw != null) try {
                bw.close();
            } catch (IOException ioe2) {
            }
        }

    }*/

    public byte[] readBlock(byte[] input)
    {
    	ReadBlockRequest r = ReadBlockRequest.parseFrom(input);
    	String fileName = r.getChunkName();
    	ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        try
        {
        	byte[] bytes = Files.readAllBytes(Paths.get(fileName));
        	response.setBytes(bytes);
        	response.setStatus(true);
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            response.setStatus(false);
        }

        return response.build().toByteArray();
    }

    public byte[] writeBlock(byte[] input)
    {
    	WriteBlockRequest w = WriteBlockRequest.parseFrom(input);
    	String fileName = w.getChunkName();
    	WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
        try
        {
        	FileOutputStream output = new FileOutputStream(fileName, false);
            output.write(w.getBytes());
            output.close(); 
            response.setStatus(true);
        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock ");
            response.setStatus(false);
        }
        return response.build().toByteArray();
    }

    public void BlockReport() throws IOException
    {
    	BlockReport.Builder b = BlockReport.newBuilder();
    	DataNodeInfo.Builder d = DataNodeInfo.newBuilder();
    	d.setId(MyID);
    	d.setName(MyName);
    	d.setIp(MyIP);
    	d.setPort(MyPort);
    	b.setDataNodeInfo(d);
    	for(String s: MyChunks) {
    		b.addChunkName(s);
    	}
    	BlockReport r = b.build();
       
    	NNStub.blockReport(b.build().toByteArray());

    	
    }

    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            registry.rebind(Name, stub);
            System.out.println("\nDataNode connected to RMIregistry\n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    public static void main(String args[]) throws InvalidProtocolBufferException, IOException
    {
    	//TODO multithreading
    	
        //Define a Datanode Me
        DataNode Me = new DataNode();   
        try {
            int port = -1;

            // create the URL to contact the rmiregistry
            String url = "//localhost:" + port + "/DataNode";
            System.out.println("binding " + url);

            // register it with rmiregistry
            Naming.rebind(url, Me);

            System.out.println("dataNode " + url + " is running...");
        }
        catch (Exception e) {
            System.out.println("dataNode failed:" + e.getMessage());
        }

    }
}
