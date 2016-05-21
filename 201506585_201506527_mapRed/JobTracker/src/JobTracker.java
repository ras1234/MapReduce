import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import com.google.protobuf.InvalidProtocolBufferException;


public class JobTracker {
	
	static int jobId;
	static int current;
	static int total;
	static int done=0;
	static String JTsearchKey;
	static String fileName="";
	static HashMap<Integer,Set<Integer>> NNConf;
	static Set<Integer> blocknum;
	static String opFileName;
	@SuppressWarnings("rawtypes")
	static Iterator it;
	// Interface to be implemented
	public interface IJobTracker extends Remote{
		
		/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
		byte[] jobSubmit(byte[] b) throws RemoteException;

		/* JobStatusResponse getJobStatus(JobStatusRequest) */
		byte[] getJobStatus(byte[] b) throws RemoteException;
		
		/* HeartBeatResponse heartBeat(HeartBeatRequest) */
		byte[] heartBeat(byte[] b) throws RemoteException;
	}
	
	public static class JobTrackerI implements IJobTracker {

		protected JobTrackerI() throws RemoteException {
			super();
		}
		
		@SuppressWarnings("unused")
		@Override
		public byte[] jobSubmit(byte[] req) throws RemoteException {
			jobId++;
			try {
				String mapName = MapReduce.JobSubmitRequest.parseFrom(req).getMapName();
				String reduceName = MapReduce.JobSubmitRequest.parseFrom(req).getReducerName();
				String inpFileName = MapReduce.JobSubmitRequest.parseFrom(req).getInputFile();
				opFileName = MapReduce.JobSubmitRequest.parseFrom(req).getOutputFile();
				int reduceTasks = MapReduce.JobSubmitRequest.parseFrom(req).getNumReduceTasks();
				fileName = inpFileName;
				
				//-----------------NameNode Remote Call-------------------
				Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56100);
		    	NameNode.INameNode namenode=(NameNode.INameNode) rmiRegistry.lookup("NameNode");
				// remote call for getting hash map
				NNConf = namenode.getBlockLocations(inpFileName);
				// block num as key set
				blocknum=NNConf.keySet();
				it = blocknum.iterator();
				total = blocknum.size();
				current = 0;
				System.out.println("Total block nums "+total);
				
				//--------------------------------------------------------
				MapReduce.JobSubmitResponse.Builder jsresp = MapReduce.JobSubmitResponse.newBuilder();
				jsresp.setJobId(jobId);
				return jsresp.build().toByteArray();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return null;
		}

		@SuppressWarnings("unused")
		@Override
		public byte[] getJobStatus(byte[] b) {
			try {
				int JobId = MapReduce.JobStatusRequest.parseFrom(b).getJobId();
				MapReduce.JobStatusResponse.Builder jsresp = MapReduce.JobStatusResponse.newBuilder();
				jsresp.setTotalMapTasks(total);
				jsresp.setNumMapTasksStarted(current);
				if(it.hasNext()==false)
					jsresp.setJobDone(true);
				else jsresp.setJobDone(false);
				return jsresp.build().toByteArray();
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public synchronized byte[] heartBeat(byte[] b) {
			MapReduce.HeartBeatResponse.Builder hbresp = MapReduce.HeartBeatResponse.newBuilder();
			try {
			if(fileName==""){
				System.out.println("Starting Irritation");
				System.out.println("called by "+MapReduce.HeartBeatRequest.parseFrom(b).getTaskTrackerId());
				return null;}
			else if( it.hasNext()==false || done == 1){ 
				System.out.println("Finished from start");
				hbresp.setStatus(100);
				System.out.println("called by "+MapReduce.HeartBeatRequest.parseFrom(b).getTaskTrackerId());
				
				return hbresp.build().toByteArray();
				}
			
			
				
				System.out.println("Giving Work to TT");
				int TTid = MapReduce.HeartBeatRequest.parseFrom(b).getTaskTrackerId();
				int freeSlots = MapReduce.HeartBeatRequest.parseFrom(b).getNumMapSlotsFree();
				
				//remote call
				Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0."+TTid,56200+TTid);
				TaskTracker.ITaskTracker taskTrackerStub=(TaskTracker.ITaskTracker) rmiRegistry.lookup("TaskTracker"+TTid);
				
				int fslot=freeSlots;
				String blocks = "";
				int workDoneThreads=0;
				while(fslot-- > 0)
				{
					if(it.hasNext())
					{
						current++;
						workDoneThreads++;
						System.out.println("current in it.next"+current);
						int keyBlock = (int) it.next();
						int valId = (int) NNConf.get(keyBlock).toArray()[0];
						blocks+=fileName+"#"+keyBlock+"#"+valId;
						blocks+="@";
						System.out.println("IT.next blocks "+blocks);
					}
					else if(!it.hasNext() && blocks==""){
						System.out.println("current "+current);
						hbresp.setStatus(100);
						System.out.println("Finished in it.next");

						done=1;
						// calling reducer
						Reducer.reduce(fileName,opFileName);
						
						return hbresp.build().toByteArray();
					}
				}
				
				System.out.println("Calling Task Tracker with threads "+workDoneThreads);
				//remote call
				System.out.println("Fslot val "+fslot+" blocks  "+blocks);
				if(blocks!="")
				{
					System.out.println("Fslot val inside remote call"+fslot);
					taskTrackerStub.taskSubmit(workDoneThreads, blocks);
				}
				if(!it.hasNext() ){
					System.out.println("current "+current);
					hbresp.setStatus(100);
					System.out.println("Finished");

					done=1;
					Thread.sleep(10000);
					// calling reducer
					Reducer.reduce(fileName,opFileName);
					
					return hbresp.build().toByteArray();
				}
				
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			hbresp.setStatus(1);
			return hbresp.build().toByteArray();
		}
		
	}//JTI
	
	
	
	public static void main(String[] args) {
		try {
			int id=0;
			jobId=0;
			try {
				BufferedReader sString = new BufferedReader(new FileReader("SearchString.txt"));
				JTsearchKey = sString.readLine();
				sString.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.setProperty("java.rmi.server.hostname", "10.0.0.1");
        	JobTrackerI obj = new JobTrackerI();
        	IJobTracker JTstub = (IJobTracker)UnicastRemoteObject.exportObject(obj, 56200+id);
        	Registry registry = LocateRegistry.createRegistry(56200+id);
          	registry.rebind("JobTracker", JTstub);

			System.out.println(" JobTracker Registered Successfully");
			
			//Thread.sleep(10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}//psvm
}
