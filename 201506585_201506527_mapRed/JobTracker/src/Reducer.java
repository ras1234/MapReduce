import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Reducer {
	@SuppressWarnings("rawtypes")
	public static void reduce(String FileName,String outputFilename)
	{
		BufferedReader buffer;
		BufferedWriter writer;
		HashMap<Integer,Set<Integer>> NNConf;
		//Set<Integer> blocknum;
		Iterator it;
		System.out.println("Inside Reducer------------- ");
		//-----------------NameNode Remote Call-------------------
		Registry rmiRegistry;
		try {
			writer = new BufferedWriter(new FileWriter(outputFilename));
			rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
			NameNode.INameNode namenode=(NameNode.INameNode) rmiRegistry.lookup("NameNode");
			// remote call for getting hash map
			// HDFS API
			NNConf = namenode.getBlockLocations(FileName);

			// block num as key set
			//blocknum=NNConf.keySet();
			
			Set<Integer> blockn=NNConf.keySet();
			List<Integer> blocknum=new ArrayList<Integer>();
			blocknum.addAll(blockn);
			Collections.sort(blocknum);
			
			it = blocknum.iterator();
			while(it.hasNext())
			{
				int blockNumber = (int) it.next();
				
				File f = new File("map"+FileName+blockNumber);
				
				System.out.println("Getting mapper temp file to local ");
				// HDFS API
				Client.get("map"+FileName+blockNumber);
				System.out.println("Reducer------------- "+"map"+FileName+blockNumber);
				
				buffer = new BufferedReader(new FileReader("map"+FileName+blockNumber));
				String line="";
				while((line=buffer.readLine())!=null)
				{
					writer.write(line+"\n");
				}
				buffer.close();
			}
			writer.close();
			
			System.out.println("Putting final op file to HDFS ");
			// HDFS API
			//Client.put(outputFilename);
			System.exit(0);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
	}
}
