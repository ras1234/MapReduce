import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

public class JobClient {
	
	public String reduce(String s)
	{
		String output="";
		return output;
	}
	
	@SuppressWarnings("resource")
	public static void main(String args[])
	{
		System.out.println("Job Client Started");		
		
		System.out.println("Enter file name to search for : ");
		Scanner inp = new Scanner(System.in);
		String fileName = inp.nextLine();
		System.out.println("Initiated for "+fileName);
		while(true)
		{
			try {
				Thread.sleep(5000);
				MapReduce.JobSubmitRequest.Builder req = MapReduce.JobSubmitRequest.newBuilder();
				req.setMapName("Mapper");
				req.setReducerName("Reducer");
				req.setInputFile(fileName);
				req.setOutputFile("outputFile");
				req.setNumReduceTasks(5);
				
  				Registry rmiRegistry = LocateRegistry.getRegistry("10.0.0.1",56200);
				JobTracker.IJobTracker jobtrackerStub=(JobTracker.IJobTracker) rmiRegistry.lookup("JobTracker");
				//remote call
				byte[] jobSubmitResponse = jobtrackerStub.jobSubmit(req.build().toByteArray());
				int jobId = MapReduce.JobSubmitResponse.parseFrom(jobSubmitResponse).getJobId();
				System.out.println("Job Id is "+jobId);
				
				MapReduce.JobStatusRequest.Builder JSreq = MapReduce.JobStatusRequest.newBuilder();
				JSreq.setJobId(jobId);
				while(true)
				{
					// remote call
					byte[] jobStatusResponse = jobtrackerStub.getJobStatus(JSreq.build().toByteArray());
					if(MapReduce.JobStatusResponse.parseFrom(jobStatusResponse).getJobDone())
					{
						System.out.println("Job Completed");
						System.exit(0);
					}
					int totMapTask = MapReduce.JobStatusResponse.parseFrom(jobStatusResponse).getTotalMapTasks();
					int started = MapReduce.JobStatusResponse.parseFrom(jobStatusResponse).getNumMapTasksStarted();
					System.out.println("Percentage of map Task = "+((double)started/(double)totMapTask));
					
					Thread.sleep(2000);
				}
				
			} catch (Exception e) {
				e.printStackTrace();}
			
			System.out.println("Do u want to continue(0/1)");
			if(inp.nextInt()==0)
				break;
		}//whiletrue		
		
	}//psvm
	

}
