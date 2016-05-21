

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class DataNode {
	
	static String BlockReport="";
	public interface IDataNode extends Remote{
		byte[] readBlock(String fname,int b) throws RemoteException;
		void writeBlock(String fname,int block,byte[] inp) throws RemoteException;	
	}
	
	@SuppressWarnings("serial")
	public static class DataNodeI implements IDataNode
	{

		protected DataNodeI() throws RemoteException {
			super();
		}

		/* ReadBlockResponse readBlock(ReadBlockRequest)) */
		/* Method to read data from any block given block-number */
		@Override
		public byte[] readBlock(String fname,int blkno) throws RemoteException {
			try{
				File file = new File(fname+":"+blkno);
				int status;
				FileInputStream f = new FileInputStream(file);
				byte[] data = new byte[(int)file.length()];
				status=1;
				f.read(data);
				return data;
			}catch(Exception e){e.printStackTrace();}
			return null;
		}

		/* WriteBlockResponse writeBlock(WriteBlockRequest) */
		/* Method to write data to a specific block */
		@Override
		public void writeBlock(String fname,int blocknum,byte[] inp) throws RemoteException {
			try{
				File file = new File(fname+":"+Integer.toString(blocknum));
				BlockReport+=fname+":"+Integer.toString(blocknum)+"\n";
				int status ;
				FileOutputStream fout = new FileOutputStream(file);
				fout.write(inp);
				status=1;
				System.out.println("File written successfully"+blocknum);
				fout.close();
			}catch(Exception e){e.printStackTrace();}
		}
		
	}
	
	@SuppressWarnings("resource")
	public static void main(String args[])
	{
		final int id;
		System.out.println("Enter Id of DataNode (1-3)");
		Scanner inp = new Scanner(System.in);
		id = inp.nextInt();
		try {
			System.setProperty("java.rmi.server.hostname", "10.0.0."+id);
        	DataNodeI obj = new DataNodeI();
        	IDataNode stub = (IDataNode)UnicastRemoteObject.exportObject(obj, 56100+id);
        	Registry registry = LocateRegistry.createRegistry(56100+id);
          	registry.rebind("DataNode"+id, stub);

			System.out.println("Data Node "+id+"Registered Successfully");
			//Thread.sleep(10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Before DataNode BlockReport");
		new Thread()
		{
			public void run()
			{
				while(true)
				{
					try {
						Thread.sleep(2000);
          				Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
						NameNode.INameNode namenode=(NameNode.INameNode) rmiRegistry.lookup("NameNode");
						//NameNode.INameNode namenode = (NameNode.INameNode)Naming.lookup("NameNode");
						//remote call
						namenode.blockReport(DataNode.BlockReport,id);
					} catch (Exception e) {
						e.printStackTrace();}
				}//whiletrue
			}
		}.start();

	}
}
