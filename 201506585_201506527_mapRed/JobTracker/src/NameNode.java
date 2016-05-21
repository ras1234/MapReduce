import java.io.File;
import java.io.FileOutputStream;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NameNode {
	static int fileHandle=-1,BlockNum;
	static HashMap<Integer,String> mymap= new HashMap<Integer,String>();
	static HashMap<String,HashMap<Integer,Set<Integer>>> NNconf = new HashMap<String,HashMap<Integer,Set<Integer>>>();
	
	public static int getFileHandle() {
		return fileHandle;
	}

	public static void setFileHandle(int fileHandle1) {
		fileHandle = fileHandle1;
	}

	public static int getBlockNum() {
		return BlockNum;
	}

	public static void setBlockNum(int blockNum1) {
		BlockNum = blockNum1;
	}

	public interface INameNode extends Remote{
		int openFile(String filename) throws RemoteException;
		byte[] closeFile(byte[] inp ) throws RemoteException;
		HashMap<Integer,Set<Integer>>  getBlockLocations(String f ) throws RemoteException;
		int assignBlock(int handle ) throws RemoteException;		
		Object[] list() throws RemoteException;
		byte[] blockReport(String rep,int id ) throws RemoteException;
		byte[] heartBeat(byte[] inp ) throws RemoteException;
	}
	
	// --------------- implementing interface--------------------
	@SuppressWarnings("serial")
	public static class NameNodeI implements INameNode
	{

		protected NameNodeI() throws RemoteException {
			super();
		}

		/* OpenFileResponse openFile(OpenFileRequest) */
		/* Method to open a file given file name with read-write flag*/
		@Override
		public int openFile(String filename) throws RemoteException {
			try {
					System.out.println("Getting File Handle---------------------"+NameNode.getFileHandle());
					// saving FIle Handle of FIle Name
					mymap.put(NameNode.getFileHandle(),filename);
					System.out.println("Saved File Name ------------"+mymap.get(NameNode.getFileHandle()) + filename);
					//inc the file handle
					NameNode.setFileHandle(NameNode.getFileHandle()+1);
					// set return status 1 for success
					return (NameNode.getFileHandle()-1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return 0;
		}

		/* CloseFileResponse closeFile(CloseFileRequest) */
		@Override
		public byte[] closeFile(byte[] inp) throws RemoteException {
			// TODO Auto-generated method stub
			return null;
		}
		
		/* BlockLocationResponse getBlockLocations(BlockLocationRequest) */
		/* Method to get block locations given an array of block numbers */
		@Override
		public HashMap<Integer,Set<Integer>>  getBlockLocations(String fname) throws RemoteException {
			int status=1;
			return NNconf.get(fname);
		}

		/* AssignBlockResponse assignBlock(AssignBlockRequest) */
		/* Method to assign a block which will return the replicated block locations */
		@Override
		public int assignBlock(int handle) throws RemoteException {
			
			try {
				// setting unique block number
				int retB = NameNode.getBlockNum();
				NameNode.setBlockNum(NameNode.getBlockNum()+1);
				int status;
				System.out.println("Error----------"+mymap.get(handle)+" Value of Handle "+handle);
				NNconf.put(mymap.get(handle),new HashMap<Integer,Set<Integer>>());
				//NNconf.put("file",new HashMap<Integer,Set<Integer>>());
				status=1;
				return retB;
			}catch(Exception e){e.printStackTrace();}
			return 0;
		}

		/* ListFilesResponse list(ListFilesRequest) */
		/* List the file names (no directories needed for current implementation */
		@Override
		public Object[] list() throws RemoteException {
			int status=1;
			return NNconf.keySet().toArray();
		}

		
		/*
		Datanode <-> Namenode interaction methods
	*/
	
	/* BlockReportResponse blockReport(BlockReportRequest) */
	/* Get the status for blocks */
		@Override
		public byte[] blockReport(String BlockReport,int id) throws RemoteException {
			if(BlockReport.isEmpty())return null;
			//first splitting lines
			String lines[]=BlockReport.split("\n");
			//HashMap<Integer,Set<Integer>> temp=new HashMap<Integer,Set<Integer>>();
			for(String line:lines){
				int blockno=Integer.parseInt(line.split(":")[1]);
				String filename=line.split(":")[0];
				HashMap<Integer,Set<Integer>> hmap;
				int status;
				hmap=NNconf.get(filename);
				//Set<Integer> dnid=new HashSet<Integer>();
				// split by : second block num
				
				Set<Integer> myset;
				//System.out.println("Inside BR "+ hmap.containsKey(blockno));
				 if(!hmap.containsKey(blockno))
				 {
					 status=1;
					  myset=new HashSet<Integer>();
					  // added condition for status
					  if(false)status=0;
					 myset.add(id);
				 }
				 else
				 {
					 status=1;
					 myset=hmap.get(blockno);
					 // added condition for status
					 if(false)status=0;
					 myset.add(id);
					 //System.out.println("Block Number # "+blockno);
				 }
				 hmap.put(blockno,myset);
				 status=1;
				 NNconf.put(filename,hmap);
			}
			return null;
		}

		/* HeartBeatResponse heartBeat(HeartBeatRequest) */
//		/* Heartbeat messages between NameNode and DataNode */
		@Override
		public byte[] heartBeat(byte[] inp) throws RemoteException {
			// TODO Auto-generated method stub
			return null;
		}
		
	}//NameNodeI
	
	@SuppressWarnings({ "unused", "resource" })
	public static void main(String args[])
	{
		NameNode.setFileHandle(1);
		NameNode.setBlockNum(1);

		
		try {
			System.setProperty("java.rmi.server.hostname", "10.0.0.1");
				NameNodeI obj = new NameNodeI();
				INameNode stub = (INameNode)UnicastRemoteObject.exportObject(obj, 56100);
				Registry registry = LocateRegistry.createRegistry(56100);
				registry.rebind("NameNode", stub);
/*
			NameNode.NameNodeI obj = new NameNodeI();
			Naming.rebind("NameNode", obj);*/
			System.out.println("Name Node Registered Successfully");
			//Thread.sleep(10000);
			
			//create NameNode.conf FIle
			FileOutputStream NameNodeConf = new FileOutputStream(new File("NameNode.conf"));
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
