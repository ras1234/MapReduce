import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

class MapTask implements Runnable
{
	 private Thread t;
	 private String threadName;
	 private String Filename;
	 static String searchKey;
	 
	public MapTask(String threadname,String filename) {
		
		try {
			BufferedReader sString = new BufferedReader(new FileReader("SearchString.txt"));
			searchKey = sString.readLine();
			sString.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Filename=filename;
		threadName = threadname;
	    System.out.println("Creating " +  threadName );
	}
	
	public synchronized void run()
	{
		System.out.println("Running"+threadName);
		try {
			String mapOutputFile="map"+Filename;
			Mapper mapObj=new Mapper();
			BufferedReader buffer=new BufferedReader(new FileReader(Filename));
			BufferedWriter writer = new BufferedWriter(new FileWriter(mapOutputFile));
			String line="";
			while((line=buffer.readLine())!=null)
			{
				String output=mapObj.map(line);
				if(output!=null)
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
	

	public synchronized void start() {
		//System.out.println("Starting " +  threadName );
	      if (t == null)
	      {
	         t = new Thread (this, threadName);
	         t.start ();
	      }
		
	}
}