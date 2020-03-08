package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;

import com.google.protobuf.ByteString; 
//import ds.hdfs.INameNode;

import ds.hdfs.Proto_Defn.ChunkLocations;
import ds.hdfs.Proto_Defn.ClientRequest;
import ds.hdfs.Proto_Defn.DataNodeInfo;
import ds.hdfs.Proto_Defn.ListResult;
import ds.hdfs.Proto_Defn.ReadBlockRequest;
import ds.hdfs.Proto_Defn.ReadBlockResponse;
import ds.hdfs.Proto_Defn.ReturnChunkLocations;

public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                continue;
            }
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
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public void PutFile(String fileName) //Put File
    {
        System.out.println("Going to put file" + fileName);
        BufferedInputStream bis;
        try{
            bis = new BufferedInputStream(new FileInputStream(fileName));
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
        
        ClientRequest.Builder c = ClientRequest.newBuilder();
    	c.setRequestType(ClientRequest.ClientRequestType.PUT);
    	c.setFileName(fileName);
    	ClientRequest r = c.build();
        //contact nameNode
        //get list of file locations
    	//for each location
    		//send chunk
    		//if failed, try other DNs
    		//if all fail, report error
    }

    public void GetFile(String fileName)
    {
    	//TODO: fill in host + port #
    	String host = null; //hostname of server
    	int port = -1; //port of rmi registry
        String url = "//" + host + ":" + port + "/NameNode";
        System.out.println("looking up " + url);
        
        INameNode nameNode = (INameNode)Naming.lookup(url);
    	ClientRequest.Builder c = ClientRequest.newBuilder();
    	c.setRequestType(ClientRequest.ClientRequestType.GET);
    	c.setFileName(fileName);
    	byte[] input = c.build().toByteArray();
    	
    	try {
    		byte[] locationBytes = nameNode.getBlockLocations(input);
    		ReturnChunkLocations fileList = ReturnChunkLocations.parseFrom(locationBytes);
    		List<ChunkLocations> locations = fileList.getLocationsList();
    		ArrayList<byte[]> streams = new ArrayList<byte[]>();
			//TODO: sort location list by sequence number
    		//loop through each chunk
    		for(ChunkLocations l: locations) {
    			String chunkName = l.getChunkName();
    			List <DataNodeInfo> list = l.getDataNodeInfoList();
    			for(DataNodeInfo d: list) {
    				int i = 0;
					//build request to DN
					ReadBlockRequest.Builder DNrequest = ReadBlockRequest.newBuilder();
			    	DNrequest.setChunkName(chunkName);
			    	byte[] r = DNrequest.build().toByteArray();
			    	
    				//loop through each location of the given chunk until DN successfully returns bytes
    				ReadBlockResponse response;
    				do {
    					//build request URL for dataNode
    					String DNurl = null; //TODO: fill in url using ip + port
    					IDataNode dataNode = (IDataNode)Naming.lookup(DNurl);

    					byte[] b = dataNode.readBlock(r);  //make request
    					response = ReadBlockResponse.parseFrom(b);
    					
    				} while(i++ < list.size() && response.getStatus()==false);
    				if(i >= list.size()) { //all failed to read
    					System.out.println("error: failed to retrieve file");
    					return;
    				}
    				streams.add(response.getBytes()); //store bytes in memory
    			}
    		}
    		FileOutputStream output = new FileOutputStream(fileName, true);
            for(byte[] b: streams) {
            	output.write(b);
            }
            output.close(); 
    	}
    	catch(RemoteException e) {
    		 System.err.println("Server Exception: " + e.toString());
             e.printStackTrace();
    	}
    	catch (Exception e) {
    		System.out.println("Exception: " + e); 
    	}    	
    }

    public void List()
    {	
    	//TODO: fill in host + port #
    	String host = null; //hostname of server
    	int port = -1; //port of rmi registry
        String url = "//" + host + ":" + port + "/NameNode";
        System.out.println("looking up " + url);
        
        INameNode nameNode = (INameNode)Naming.lookup(url);
        
    	ClientRequest.Builder c = ClientRequest.newBuilder();
    	c.setRequestType(ClientRequest.ClientRequestType.LIST);
    	ClientRequest r = c.build();
    	byte[] input = r.toByteArray();
    	try {
    		byte[] resultBytes = nameNode.list(input);
    		ListResult fileList = ListResult.parseFrom(resultBytes);
    		for(String s: fileList.getFileNameList()) {
    			System.out.println(s);
    		}
    	}
    	catch(RemoteException e) {
    		 System.err.println("Server Exception: " + e.toString());
             e.printStackTrace();
    	}
    	
    }

    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        Client Me = new Client();
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
