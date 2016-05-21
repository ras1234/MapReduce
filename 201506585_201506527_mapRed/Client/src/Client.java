import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.io.DataInputStream;

public class Client {
	static long BlockSize=33554432;//32*1024*1024
	static Registry rmiRegistry;

	@SuppressWarnings({ "resource" })
	public static void main(String args[])
	{
		while(true)
		{
			System.out.println("Select Choices (1-3) ");
			System.out.println("1 - Put\n 2 - Get \n 3 - List");
			Scanner inp = new Scanner(System.in);
			int enteredChoice = inp.nextInt();
			String fname="";
			switch(enteredChoice)
			{
				// put
				case 1:System.out.println("Enter Pathname of File to be uploaded");
					fname = inp.nextLine();
					fname = inp.nextLine();
					put(fname);
					break;
	
					//get
				case 2: System.out.println("Enter Pathname of File to be displayed");
					fname = inp.nextLine();
					fname = inp.nextLine();
					get(fname);
					break;
					
					//list
				case 3:System.out.println("List of files Currently on HDFS");
					list();
					
			}//switch
			System.out.println("DO you want to continue ?? (1 for yes / 0 for No)");
			if(inp.nextInt()==0)
				break;
		}//while
		 
	}//psvm
	public static void put(String fname)
	{
		File file = new File(fname);
		Random rand = new Random(4);
		long FileSize = file.length();
		try {
			DataInputStream inpstream = new DataInputStream(new FileInputStream(file));
			int NumOfBlocks = (int)Math.ceil(FileSize*1.0/BlockSize);
			int rem = (int) (FileSize - (NumOfBlocks-1)*BlockSize);
			byte[] data = new byte[(int)BlockSize];
			byte[] remdata = new byte[rem];
			// looking up NameNode
			rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
    	    NameNode.INameNode namenode=(NameNode.INameNode) rmiRegistry.lookup("NameNode");

			//NameNode.INameNode namenode = (NameNode.INameNode) Naming.lookup("NameNode");
			System.out.println("lookup successful");
			//remote call to NameNode OpenFileReq
			int handle = namenode.openFile(fname);
			System.out.println("Handle------"+handle);

			System.out.println("handle "+handle+" Num of Blocks "+NumOfBlocks);
			
			for(int i=0;i<NumOfBlocks;i++)
			{
				int blknum = namenode.assignBlock(handle);
				System.out.println("Block Num "+blknum);
				// reading 32 Mb from local file 
				if(i==NumOfBlocks-1)
					inpstream.read(remdata);
				else
					inpstream.read(data);
				int r = rand.nextInt(4) + 1;
				//r=1;
				System.out.println("Value of R = "+r);
				rmiRegistry = LocateRegistry.getRegistry("10.0.0."+(r),56100+(r));
				DataNode.IDataNode datanode1=(DataNode.IDataNode) rmiRegistry.lookup("DataNode"+r);
				//DataNode.IDataNode datanode = (DataNode.IDataNode)Naming.lookup("DataNode"+r);
				
				System.out.println("Remote Req"+blknum+" "+data);
				//remote call for Write to DataNode
				if(i==NumOfBlocks-1)
					datanode1.writeBlock(fname,blknum, remdata);
				else
					datanode1.writeBlock(fname,blknum,data);
				
				int r2=0;
				// replicate to two data Nodes
				while( (r2=rand.nextInt(4)+1) == r);
				r = r2;
				System.out.println("Value of R2 = "+r);
				rmiRegistry = LocateRegistry.getRegistry("10.0.0."+(r),56100+(r));
				DataNode.IDataNode datanode2=(DataNode.IDataNode) rmiRegistry.lookup("DataNode"+r);
				//DataNode.IDataNode datanode = (DataNode.IDataNode)Naming.lookup("DataNode"+r);
				System.out.println("Remote Req"+blknum+" "+data);
				//remote call for Write to DataNode
				if(i==NumOfBlocks-1)
					datanode2.writeBlock(fname,blknum, remdata);
				else
					datanode2.writeBlock(fname,blknum,data);

			}
			inpstream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}// put
	
	 public static void get(String filename){
		 		
		    try {
		    	FileOutputStream getFile = new FileOutputStream(new File(filename));
		    	
		    	rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
		    	NameNode.INameNode node=(NameNode.INameNode) rmiRegistry.lookup("NameNode");
				//HashMap<Integer,Set<Integer>> h =node.getBlockLocations(filename);

				//NameNode.INameNode node=(NameNode.INameNode) Naming.lookup("NameNode");
				// remote call for getting hash map
				HashMap<Integer,Set<Integer>> NNConf =node.getBlockLocations(filename);
				if(NNConf==null)
					System.out.println("Client NNConf --------NNconf is null");
				// block num as key set
				Set<Integer> blockn=NNConf.keySet();
				if(blockn==null)
					System.out.println("Client NNConf ------ blockn is null");
				List<Integer> blocknum=new ArrayList<Integer>();
				blocknum.addAll(blockn);
				Collections.sort(blocknum);
				
				for(Integer blockNo:blocknum)
				{
					System.out.println("On block Num "+blockNo);
					Set<Integer> Datanode=NNConf.get(blockNo);
					int id = (int)Datanode.toArray()[0];
	    	    
	    	    	rmiRegistry = LocateRegistry.getRegistry("10.0.0."+id,56100+id);
    	    	 	DataNode.IDataNode node1=(DataNode.IDataNode) rmiRegistry.lookup("DataNode"+id);

	    	    	//DataNode.IDataNode node1=(DataNode.IDataNode) Naming.lookup("DataNode"+id);
	    	    	 // remote call for read
	    	    	 byte block[]=node1.readBlock(filename, blockNo);
	    	    	 //writing to the file as merged
	    	    	 getFile.write(block);
	    	    	 
				}
				getFile.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}//get 
	 public static void list()
	    {
	    	try{
	    		rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
    			NameNode.INameNode namenode=(NameNode.INameNode) rmiRegistry.lookup("NameNode");

	    	//NameNode.INameNode namenode=(NameNode.INameNode) Naming.lookup("NameNode");
	    	//byte[] data=new byte[10];
	    	//remote call
	    	Object files[]=namenode.list();
	    	for(Object File:files){
	    		// printing files one by one
	    		String f= (String)File;
	    		System.out.println(f);
	    		}
	    	}
	    	catch(Exception e){e.printStackTrace();}
	    }//list
	 
		    
}
