import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;


	public class TaskTracker {
	
	static final int numThreads=5;
	static int freeThreads;
	public interface ITaskTracker extends Remote{
		/* taskSubmit */
		public void taskSubmit(int blockName,String b) throws RemoteException;
	}
	
	public static class TaskTrackerI implements ITaskTracker {

		protected TaskTrackerI() throws RemoteException {
			super();
		}

		@Override
		public void taskSubmit(int tasks,String blocks) {
			System.out.println("Original free Threads "+freeThreads+ " Tasks req "+tasks);
			freeThreads -=tasks;
			System.out.println("Number of free threads now are "+freeThreads);
			try{
				System.out.println("Task "+blocks+" Submit");
				if(blocks=="")return;
			//create a thread pool for number of Map task
				int ite=0;
			for(String fname:blocks.split("@"))
			{
				ite++;
				System.out.println("In iteration "+ite);
				if(fname=="")break;
				String[] temp = fname.split("#");
				String ttfname = temp[0];
				int ttBlock = Integer.parseInt(temp[1]);
				int ttId = Integer.parseInt(temp[2]);
				FileOutputStream getFile = new FileOutputStream(new File(ttfname+ttBlock));
				Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0."+ttId,56100+ttId);
	    	 	DataNode.IDataNode node1=(DataNode.IDataNode) rmiRegistry.lookup("DataNode"+ttId);
    	    	 // remote call for read
    	    	 byte block[]=node1.readBlock(ttfname, ttBlock);
    	    	 //writing to the file as merged
    	    	 getFile.write(block);
    	    	 getFile.close();
    	    	 
				MapTask th = new MapTask("Thread#"+ttfname+ttBlock,ttfname+ttBlock);
				th.start();
				//run1("Thread#"+ttfname+ttBlock,ttfname+ttBlock);
				//System.out.println("Started Thread:"+x);
			}
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
		
		
	}//TTI
	public static void run1(String threadName,String Filename)
	{
		//String SearchKey = JobTracker.JTsearchKey;
		String SearchKey = "chronicles";
		System.out.println("Search Key = "+SearchKey);
		System.out.println("Running"+threadName);
		try {
			String mapOutputFile="map"+Filename;
			//Mapper mapObj=new Mapper();
			BufferedReader buffer=new BufferedReader(new FileReader(Filename));
			BufferedWriter writer = new BufferedWriter(new FileWriter(mapOutputFile));
			String line="";
			while((line=buffer.readLine())!=null)
			{
				if(line.contains(SearchKey))
				{
					writer.write(line+"\n");
				}
			}
			
			buffer.close();
			writer.close();
			System.out.println("Done");
			
			System.out.println("Putting in HDFS map temp file "+mapOutputFile);
			// putting in HDFS
			Client.put(mapOutputFile);
			
			TaskTracker.freeThreads++;
			System.out.println("Destroy "+TaskTracker.freeThreads);
		} catch (Exception e) {e.printStackTrace();}
		
	}//run


	@SuppressWarnings("resource")
	public static void main(String[] args)  {
		try {			
			//Thread.sleep(5000);
			freeThreads = numThreads;
			Scanner inp = new Scanner(System.in);
			System.out.println("Enter Id (1-4)");
			final int id=inp.nextInt();
			
			System.setProperty("java.rmi.server.hostname", "10.0.0."+id);
        	TaskTrackerI obj = new TaskTrackerI();
        	ITaskTracker TTstub = (ITaskTracker)UnicastRemoteObject.exportObject(obj, 56200+id);
        	Registry registry = LocateRegistry.createRegistry(56200+id);
          	registry.rebind("TaskTracker"+id, TTstub);

			System.out.println(" TaskTracker "+id+" Registered Successfully");
			Timer t = new Timer();
			t.scheduleAtFixedRate(
			    new TimerTask()
			    {
			        public void run()
			        {
			        	try{
			            System.out.println("HeartBeat dhak dhak");
			          //remote call to JT
						Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56200);
						JobTracker.IJobTracker jobTrackerStub=(JobTracker.IJobTracker) rmiRegistry.lookup("JobTracker");
						System.out.println("Calling Job Tracker");
						MapReduce.HeartBeatRequest.Builder hbreq = MapReduce.HeartBeatRequest.newBuilder();
						hbreq.setTaskTrackerId(id);
						System.out.println("Free Threads "+freeThreads);
						hbreq.setNumMapSlotsFree(TaskTracker.freeThreads);
						//remote call
						byte[] hbresp = jobTrackerStub.heartBeat(hbreq.build().toByteArray());
						if(hbresp!=null)
						{
							int status = MapReduce.HeartBeatResponse.parseFrom(hbresp).getStatus();
							if(status==100)
							{
								System.exit(0);
							}
						}
						
			        	}catch(Exception e){e.printStackTrace();}
			        }
			    },
			    0,      // run first occurrence immediatetly
			    5000);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}//psvm
	

}//TT
