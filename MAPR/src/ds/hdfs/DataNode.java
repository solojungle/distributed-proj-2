package ds.hdfs;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

import ds.hdfs.Proto_Defn.BlockReport;
import ds.hdfs.Proto_Defn.DataNodeInfo;
import ds.hdfs.Proto_Defn.ReadBlockRequest;
import ds.hdfs.Proto_Defn.ReadBlockResponse;
import ds.hdfs.Proto_Defn.WriteBlockRequest;
import ds.hdfs.Proto_Defn.WriteBlockResponse;;

public class DataNode implements IDataNode {
	protected INameNode NNStub;
	protected String MyIP;
	protected int MyPort;
	protected String MyHost;
	protected String MyName;
	protected int MyID;
	protected long MyInterval; //how often DN will send blockReports (in milliseconds)

	private String MyDir;
	private TreeSet<String> MyChunks;

	public DataNode() throws RemoteException {
		// initialize values
		MyID = -1;
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			MyIP = ip.toString();
			MyHost = ip.getHostName();
			MyName = MyHost+"/DataNode";
		} catch (java.net.UnknownHostException e1) {
			e1.printStackTrace();
		}


		try {
			// parse dn_config.txt
			String line = Files.readAllLines(Paths.get("src/dn_config.txt")).get(1);
			String[] fields = line.split(";");
			MyInterval = Long.parseLong(fields[0]);
			MyPort = Integer.parseInt(fields[1]);
			
			//parse nn_config.txt
			String nnName = null;
			String nnIP = null;
			int nnPort = -1;
			List<String> lines = Files.readAllLines(Paths.get("src/nn_config.txt"));
			for(String s: lines){
				String[] split = s.split("=");
				String attr = split[0];
				switch(attr){
					case "name":
						nnName = split[1];
						break;
					case "ip":
						nnIP = split[1];
						break;
					case "port":
						nnPort = Integer.parseInt(split[1]);
				}
			}
			if(nnName == null || nnIP == null || nnPort == -1){
				System.err.println("error parsing nn_config.txt");
			}

			//look up NameNode
			NNStub = GetNNStub(nnName, nnIP, nnPort);

		} catch (IOException e) {
			System.err.println("error reading config files");
			e.printStackTrace();
		}


		// load chunks list into memory
		MyDir = "DataNode."+MyHost;
		File dir = new File(MyDir);
		dir.mkdir(); // make directory if doesn't exist
		String[] files = dir.list();
		MyChunks = new TreeSet<String>();
		for (String f : files) {
			MyChunks.add(f);
		}
	}

	// IGNORE FOR THIS PART OF PROJECT
	/*
	 * public static void appendtoFile(String Filename, String Line) {
	 * BufferedWriter bw = null;
	 * 
	 * try { //append } catch (IOException ioe) { ioe.printStackTrace(); } finally {
	 * // always close the file if (bw != null) try { bw.close(); } catch
	 * (IOException ioe2) { } }
	 * 
	 * }
	 */

	public byte[] readBlock(byte[] input) {
		// read request
		ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
		try {
			ReadBlockRequest r = ReadBlockRequest.parseFrom(input);
			String fileName = r.getChunkName();

			// check if has file
			if (!MyChunks.contains(fileName)) {
				System.out.println("Error: " + fileName + " not found");
				response.setStatus(false);
				return response.build().toByteArray();
			}

			// read and send bytes
			String path = MyDir + fileName;
			byte[] bytes = Files.readAllBytes(Paths.get(path));
			response.setBytes(ByteString.copyFrom(bytes));
			response.setStatus(true);
		} catch (Exception e) {
			System.out.println("Error at readBlock");
			response.setStatus(false);
		}

		return response.build().toByteArray();
	}

	public byte[] writeBlock(byte[] input) {

		// read request
		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
		try {
			WriteBlockRequest w = WriteBlockRequest.parseFrom(input);
			String fileName = w.getChunkName();

			// write bytes to file
			FileOutputStream output = new FileOutputStream(fileName, false);
			output.write(w.getBytes().toByteArray());
			output.close();

			// add to list of chunks
			MyChunks.add(fileName);

			response.setStatus(true);
		} catch (Exception e) {
			System.out.println("Error at writeBlock ");
			response.setStatus(false);
		}
		return response.build().toByteArray();
	}


	public void BlockReport() throws IOException {
		BlockReport.Builder b = BlockReport.newBuilder();
		DataNodeInfo.Builder d = DataNodeInfo.newBuilder();
		d.setId(MyID);
		d.setName(MyName);
		d.setIp(MyIP);
		d.setPort(MyPort);
		b.setDataNodeInfo(d);
		for (String f : MyChunks) {
			b.addChunkName(f);
		}

		NNStub.blockReport(b.build().toByteArray());

	}
/*
	public void BindServer(String Name, String IP, int Port) {
		try {
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
			System.setProperty("java.rmi.server.hostname", IP);
			serverRegistry = LocateRegistry.createRegistry(Port);	
			Registry registry = LocateRegistry.getRegistry(Port);
			registry.rebind(Name, stub);
			System.out.println("\nDataNode connected to RMIregistry\n");
		} catch (Exception e) {
			System.err.println("Server Exception: " + e.toString());
			e.printStackTrace();
		}
	}*/

	public INameNode GetNNStub(String Name, String IP, int Port) {
		while (true) {
			try {
				Registry registry = LocateRegistry.getRegistry(IP, Port);
				INameNode stub = (INameNode) registry.lookup(Name);
				System.out.println("NameNode Found!");
				return stub;
			} catch (Exception e) {
				System.out.println("NameNode still not Found");
				continue;
			}
		}
	}


	void BlockReportLoop() {
		Thread t = new Thread(() -> {
			while (true) {
				try {
					System.out.println("Sending block report");
					BlockReport();
					Thread.sleep(MyInterval);
				} catch (IOException | InterruptedException e) {
					System.out.println("Error sending block report");
				}
			}
		});
		t.start();
	}

	public static void main(String args[]) throws InvalidProtocolBufferException, IOException {

		// Define a Datanode Me
		DataNode Me = new DataNode();

		// connect with rmi registry
		try {
			// create the URL to contact the rmiregistry
			String url = Me.MyName;
			System.out.println("binding " + url);

			// register it with rmiregistry
			Registry serverRegistry = LocateRegistry.createRegistry(Me.MyPort);	
			Registry registry = LocateRegistry.getRegistry();
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(Me, 0);
			registry.bind(url, stub);

			System.out.println("dataNode " + url + " is running...");

			// spawn off block report thread
			Me.BlockReportLoop();
		} catch (Exception e) {
			System.out.println("dataNode failed:" + e.getMessage());
		}

	}
}
