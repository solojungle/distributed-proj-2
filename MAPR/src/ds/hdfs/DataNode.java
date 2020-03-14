//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import ds.hdfs.Proto_Defn.BlockReport;
import ds.hdfs.Proto_Defn.DataNodeInfo;
import ds.hdfs.Proto_Defn.ReadBlockRequest;
import ds.hdfs.Proto_Defn.ReadBlockResponse;
import ds.hdfs.Proto_Defn.WriteBlockRequest;
import ds.hdfs.Proto_Defn.WriteBlockResponse;;

public class DataNode implements IDataNode
{
    //protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int MyID;
    
    private String MyDir;
    private Map<String,Boolean> MyChunks;
    
    public DataNode()
    {
    	//look up NameNode
    	try {
			String line = Files.readAllLines(Paths.get("nn_config.txt")).get(1);
			String[] fields = line.split(";");
			NNStub = GetNNStub(fields[0],fields[1],Integer.parseInt(fields[2]));
			
		} catch (Exception e) {
			System.err.println("error reading nn_config.txt");
			e.printStackTrace();
		}
    	
    	//load chunks list into memory
    	File dir = new File(MyDir);
		String[] files = dir.list();
		Arrays.sort(files);
		MyChunks = new HashMap<String,Boolean>();
		for(String f: files) {
			MyChunks.put(f, true);
		}
    	
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
        ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        try
        {
        ReadBlockRequest r = ReadBlockRequest.parseFrom(input);
    	String fileName = r.getChunkName();
        
        if(MyChunks.get(fileName)==null) {
        	 System.out.println("Error: "+ fileName + " not found");
             response.setStatus(false);
             return response.build().toByteArray();
        }
        
    	String path = MyDir + fileName;
       	byte[] bytes = Files.readAllBytes(Paths.get(path));
       	response.setBytes(ByteString.copyFrom(bytes));
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

		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
        try
        {
        	WriteBlockRequest w = WriteBlockRequest.parseFrom(input);
    		String fileName = w.getChunkName();
    		
        	FileOutputStream output = new FileOutputStream(fileName, false);
            output.write(w.getBytes().toByteArray());
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
        	int port = Integer.parseInt(args[0]); //get port no from first cmd arg

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
