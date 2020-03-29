package ds.hdfs;

import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import ds.hdfs.Proto_Defn.ChunkLocations;
import ds.hdfs.Proto_Defn.ClientRequest;
import ds.hdfs.Proto_Defn.DataNodeInfo;
import ds.hdfs.Proto_Defn.ListResult;
import ds.hdfs.Proto_Defn.ReadBlockRequest;
import ds.hdfs.Proto_Defn.ReadBlockResponse;
import ds.hdfs.Proto_Defn.ReturnChunkLocations;
import ds.hdfs.Proto_Defn.WriteBlockRequest;
import ds.hdfs.Proto_Defn.WriteBlockResponse;

public class Client {
	// Variables Required
	public INameNode NNStub; // Name Node stub
	public IDataNode DNStub; // Data Node stub

	public Client() {
		try {
			// parse nn_config.txt
			String nnName = null;
			String nnIP = null;
			int nnPort = -1;
			List<String> lines = Files.readAllLines(Paths.get("src/nn_config.txt"));
			for (String s : lines) {
				String[] split = s.split("=");
				String attr = split[0];
				switch (attr) {
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
			if (nnName == null || nnIP == null || nnPort == -1) {
				System.err.println("error parsing nn_config.txt");
			}
			// look up NameNode
			NNStub = GetNNStub(nnName, nnIP, nnPort);

		} catch (Exception e) {
			System.err.println("error reading nn_config.txt");
			e.printStackTrace();
		}
	}

	public IDataNode GetDNStub(String Name, String IP, int Port) {
		while (true) {
			try {
				Registry registry = LocateRegistry.getRegistry(IP, Port);
				IDataNode stub = (IDataNode) registry.lookup(Name);
				System.out.println("found DataNode! " + Name);
				return stub;
			} catch (Exception e) {
				System.out.println("haven't found DataNode yet. " + Name);
				continue;
			}
		}
	}

	public INameNode GetNNStub(String Name, String IP, int Port) {
		while (true) {
			try {
				Registry registry = LocateRegistry.getRegistry(IP, Port);
				INameNode stub = (INameNode) registry.lookup(Name);
				System.out.println("found NameNode!");
				return stub;
			} catch (Exception e) {
				System.out.println("haven't found NameNode yet");
				continue;
			}
		}
	}

	public void PutFile(String localFile, String hdfsFile) // Put File
	{

		// open file
		System.out.println("Going to put file: " + localFile + " into HDFS as: " + hdfsFile);
		BufferedInputStream bis;
		try {
			bis = new BufferedInputStream(new FileInputStream(localFile));
		} catch (Exception e) {
			System.out.println("File not found !!!");
			return;
		}
		File file = new File(localFile);

		// contact nameNode
		ClientRequest.Builder c = ClientRequest.newBuilder();
		c.setFileName(hdfsFile);
		c.setFileSize(file.length());
		byte[] input = c.build().toByteArray();

		// send request
		byte[] NNresponse;
		ReturnChunkLocations fileList;
		int blockSize;
		try {
			NNresponse = NNStub.assignBlock(input);
			fileList = ReturnChunkLocations.parseFrom(NNresponse);

			// check for errors
			if (fileList.getStatus() == false) {
				if (fileList.hasError()) {
					if (fileList.getError() == ReturnChunkLocations.ErrorCode.FILE_ALREADY_EXISTS) {
						System.out.println("Error: '" + hdfsFile + "' already exists in HDFS");
					} else if (fileList.getError() == ReturnChunkLocations.ErrorCode.NOT_ENOUGH_SERVERS) {
						System.out.println("Error: there are not enough available servers right now");
					}
				} else {
					System.out.println("Error getting chunk locations");
				}
				return;
			}

			blockSize = fileList.getBlockSize();
		} catch (Exception e) {
			System.out.println("Error contacting nameNode");
			e.printStackTrace();
			return;
		}

		// split file into chunks
		byte[] buffer = new byte[blockSize];
		ArrayList<byte[]> chunks = new ArrayList<byte[]>();
		try {
			while (true) {
				int bytesAmount = bis.read(buffer);
				if (bytesAmount <= 0) {
					break;
				}
				chunks.add(Arrays.copyOf(buffer, bytesAmount));
			}

			if (chunks.size() == 0) { // if empty file, send empty byte array to one dataNode
				chunks.add(new byte[0]);
			}
		} catch (Exception e) {
			System.out.println("Error reading input file");
			e.printStackTrace();
			return;
		}

		// send chunks to each dataNode
		List<ChunkLocations> locations = fileList.getLocationsList();
		int chunkNum = 0;

		for (ChunkLocations l : locations) {
			String chunkName = l.getChunkName();
			List<DataNodeInfo> list = l.getDataNodeInfoList();

			// build request to DN
			WriteBlockRequest.Builder DNrequest = WriteBlockRequest.newBuilder();
			DNrequest.setChunkName(chunkName);
			DNrequest.setBytes(ByteString.copyFrom(chunks.get(chunkNum)));
			byte[] r = DNrequest.build().toByteArray();

			// loop through each location of the given chunk until DN successfully returns
			// bytes
			WriteBlockResponse response = null;
			for (DataNodeInfo d : list) {
				IDataNode dataNode = GetDNStub(d.getName(), d.getIp(), d.getPort());
				try {
					byte[] b = dataNode.writeBlock(r); // make request
					response = WriteBlockResponse.parseFrom(b);
					if (response.getStatus() == false) {
						System.out.println("Write failed: error contacting dataNode: " + d.getName());
						return;
					}
				} catch (Exception e) {
					System.out.println("Write failed: error contacting dataNode: " + d.getName());
					return;
				}
			}
			chunkNum++;
		}
	}

	public void GetFile(String hdfsFile, String localFile) {

		System.out.println("Going to get file: " + hdfsFile + " from HDFS as: " + localFile);

		// check if file already exists and give warning if so
		File f = new File(localFile);
		boolean exists = f.exists();
		if (exists) {
			System.out.println("Warning: '" + localFile + "' will be overwritten. Continue? [y/n]");
			Scanner in = new Scanner(System.in);
			String answer = in.nextLine().trim().toLowerCase();
			while (true) {
				if (answer.equals("y")) {
					f.delete();
					break;
				} else if (answer.equals("n")) {
					System.out.println("Aborting get operation");
					return;
				}
			}
		}

		// build request
		ClientRequest.Builder c = ClientRequest.newBuilder();
		c.setFileName(hdfsFile);
		byte[] input = c.build().toByteArray();

		// send request
		byte[] NNresponse;
		ReturnChunkLocations fileList;
		try {
			NNresponse = NNStub.getBlockLocations(input);
			fileList = ReturnChunkLocations.parseFrom(NNresponse);
			if (fileList.getStatus() == false) {
				System.out.println("Error: no servers available");
				return;
			}
		} catch (Exception e1) {
			System.out.println("Error contacting nameNode");
			e1.printStackTrace();
			return;
		}

		List<ChunkLocations> locations = fileList.getLocationsList();
		ArrayList<byte[]> streams = new ArrayList<byte[]>();

		/*
		 * //print chunk/replica info for(ChunkLocations l: locations){ int count = 0;
		 * System.out.println("----"+l.getChunkName()+"----"); for(DataNodeInfo dn :
		 * l.getDataNodeInfoList()){
		 * System.out.println("replica "+count+": "+dn.getName()); count++; } }
		 */
		// loop through each chunk
		for (ChunkLocations l : locations) {
			String chunkName = l.getChunkName();
			List<DataNodeInfo> list = l.getDataNodeInfoList();

			// build request to DN
			ReadBlockRequest.Builder DNrequest = ReadBlockRequest.newBuilder();
			DNrequest.setChunkName(chunkName);
			byte[] r = DNrequest.build().toByteArray();

			// loop through each location of the given chunk until DN successfully returns
			// bytes
			ReadBlockResponse response = null;
			int i = 0;
			do {
				DataNodeInfo d = list.get(i);
				IDataNode dataNode = GetDNStub(d.getName(), d.getIp(), d.getPort());
				byte[] b;
				try {
					b = dataNode.readBlock(r);
					response = ReadBlockResponse.parseFrom(b);
				} catch (RemoteException | InvalidProtocolBufferException e) {
					System.out.println("error contacting dataNode: " + d.getName());
				}

			} while (++i < list.size() && response.getStatus() == false);

			if (response.getStatus() == false) { // all failed to read
				System.out.println("error: failed to retrieve file");
				return;
			}
			streams.add(response.getBytes().toByteArray()); // store bytes in memory
		}

		// write file locally
		FileOutputStream output;
		try {
			output = new FileOutputStream(localFile, true);
			for (byte[] b : streams) {
				output.write(b);
			}
			output.close();
		} catch (Exception e) {
			System.out.println("error writing file locally");
			e.printStackTrace();
		}

	}

	public void List() {
		// build request
		ClientRequest.Builder c = ClientRequest.newBuilder();
		ClientRequest r = c.build();
		byte[] input = r.toByteArray();

		// print results
		try {
			byte[] resultBytes = NNStub.list(input);
			ListResult fileList = ListResult.parseFrom(resultBytes);

			for (String s : fileList.getFileNameList()) {
				System.out.println(s);
			}
		} catch (RemoteException | InvalidProtocolBufferException e) {
			System.err.println("Server Exception: " + e.toString());
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws RemoteException, UnknownHostException {
		// To read config file and Connect to NameNode
		// Intitalize the Client
		Client Me = new Client();
		System.out.println("Welcome to HDFS!!");
		Scanner Scan = new Scanner(System.in);
		while (true) {
			// Scanner, prompt and then call the functions according to the command
			System.out.print("$> "); // Prompt
			String Command = Scan.nextLine();
			String[] Split_Commands = Command.split(" ");

			if (Split_Commands[0].equals("help")) {
				System.out.println("The following are the Supported Commands");
				System.out.println(
						"1. put local_file hdfs_file ## To put a local file local_file in HDFS with name hdfs_file");
				System.out.println(
						"2. get hdfs_file local_file  ## To get a file hdfs_file from HDFS and save locally as local_file");
				System.out.println("3. list ## To get the list of files in HDFS");
				System.out.println("4. quit ## exit client");
			} else if (Split_Commands[0].equals("put")) // put Filename
			{
				// Put file into HDFS
				try {
					String localFile = Split_Commands[1];
					String hdfsFile = Split_Commands[2];
					Me.PutFile(localFile, hdfsFile);
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Please type 'help' for instructions");
					continue;
				}
			} else if (Split_Commands[0].equals("get")) {
				// Get file from HDFS
				try {
					String hdfsFile = Split_Commands[1];
					String localFile = Split_Commands[2];
					Me.GetFile(hdfsFile, localFile);
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Please type 'help' for instructions");
					continue;
				}
			} else if (Split_Commands[0].equals("list")) {
				System.out.println("List request");
				// Get list of files in HDFS
				Me.List();
			} else if (Split_Commands[0].equals("quit")) {
				return;
			} else {
				System.out.println("Please type 'help' for instructions");
			}
		}
	}
}
