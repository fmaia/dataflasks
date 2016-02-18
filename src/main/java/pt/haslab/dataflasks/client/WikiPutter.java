package pt.haslab.dataflasks.client;

import java.io.*;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pt.haslab.dataflasks.common.DFLogger;
import pt.haslab.dataflasks.loadbalancing.DynamicLoadBalancer;
import pt.haslab.dataflasks.loadbalancing.LBPassiveThread;
import pt.haslab.dataflasks.loadbalancing.LoadBalancer;

import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Collections;

public class WikiPutter
{
	private static Pattern patternId = Pattern.compile( "<id>([0-9]+)</id>" );

	public static Map<String, Wvalue> versions = new HashMap<String, Wvalue>();
	public static ReentrantLock versionslock = new ReentrantLock();

	public static LinkedList<String> files = new LinkedList<String>();
	public static ReentrantLock fileslock = new ReentrantLock();

	private static int init_version = 0;

	public static Client dfcli;

	public static void main( final String[] args )
	{
		try{	
		String folderPath = args[0];
		String myip = args[1];
		int myport = new Integer(args[2]);
		Long clienttimeout = Long.parseLong(args[3]);
		String bootip = args[4];
		Long lbinterval = Long.parseLong(args[5]);
		int nrsnaps =new Integer(args[6]);
		String absoluteFolderPath = null;
		
		//Logger
		DFLogger log = new DFLogger("wikiload."+myip,"debug");
		//Starting load balancer
		LoadBalancer lb = new DynamicLoadBalancer(log, new Random(),bootip,myip,lbinterval);
		new Thread(new LBPassiveThread((DynamicLoadBalancer) lb,myip,log)).start();
		new Thread((Runnable) lb).start();
		
		log.info("Log set up.");
		
		//Starting DataFlasks Client
		int nputreplies = 1;
		//The Client will always have the id 0 - req id is distinguished by PORT
		int senderport = 1000;
		System.out.println("[WIKILOAD] Initializing WikiLoader.");
		dfcli = new Client(new Long(0).toString(),lb,myip,myport,senderport,nputreplies,clienttimeout,log);

		log.info("DataFlasks client set up.");
		
		
		init_version=new Integer(0);

		for(int is=1; is<= nrsnaps;is++){

			String snaps = Integer.toString(is);

			File folder = new File( folderPath + "snap" + snaps);

			if ( !folder.isDirectory() )
			{
				throw new InvalidParameterException( "Given path is not a directory" );
			}

			absoluteFolderPath = folder.getAbsolutePath();

			System.out.println("[WIKILOAD] Processing Snapshot " + absoluteFolderPath);


			parseFiles( folder );

			Collections.sort(files);
			System.out.println("[WIKILOAD] Number of files considered: "+files.size());
			//launch threads
			Thread[] t=new Thread[30];

			for(int i=0;i<30;i++){
				t[i] = new Thread(new Wthread());
				t[i].start();
			}

			try{
				for(int i=0;i<30;i++){
					t[i].join();
				}
			}catch(Exception e){

				e.printStackTrace();
			}

		}
		}catch(Throwable e){
			e.printStackTrace();
		}
	}


	/**
	 * Parses a directory of articles
	 *
	 * @param folder Articles folder
	 */
	public static void parseFiles( File folder )
	{

		for ( final File file : folder.listFiles() )
		{
			if ( file.isDirectory() )
			{
				parseFiles( file );
			}
			else
			{
				files.add(file.getAbsolutePath());
			}
		}
	}

	/**
	 * Parses articles XML file
	 *
	 * @param file XML file
	 */
	public static void parseFile( File file )
	{
		try
		{
			String line = null;
			BufferedReader reader = new BufferedReader( new FileReader( file ) );
			ByteArrayOutputStream output = new ByteArrayOutputStream();


			long newrevisions=0;
			long newfiles=0;
			long equalfiles=0;

			boolean inPage = false;
			boolean gotId = false;
			boolean gotRevId = false;

			boolean newrev = true;
			String id = null;
			String revid = null;
			//System.out.println("Read file "+file.getName());
			while ( ( line = reader.readLine() ) != null )
			{
				line = line.trim();

				if ( inPage )
				{
					output.write( line.getBytes( "UTF-8" ) );
				}

				// New article is started
				if ( !inPage && line.matches( "<page>" ) )
				{
					output.write( line.getBytes( "UTF-8" ) );
					inPage = true;
				}
				// Grab article ID
				else if ( !gotId && line.matches( "<id>([0-9]+)</id>" ) )
				{
					Matcher m = patternId.matcher( line );

					if ( m.find() )
					{
						id = m.group( 1 ).toLowerCase();
						gotId = true;
					}
				}
				// Grab article revision
				else if ( !gotRevId && gotId && line.matches( "<id>([0-9]+)</id>" ) )
				{   
					Matcher m = patternId.matcher( line );

					if ( m.find() )
					{   
						revid = m.group( 1 ).toLowerCase();
						gotRevId = true;
					}

				}
				// Close article and save it
				else if ( inPage && line.matches( "</page>" ) )
				{
					Wvalue value=new Wvalue(init_version,revid);

					versionslock.lock();
					// Article does not have a previous version
					if ( !versions.containsKey( id ) )
					{
						newfiles++;
						versions.put( id, value );
					}
					// Increment article's ID
					else
					{
						value = versions.get( id );
						if(!value.revision.equals(revid)){
							value.version++;
							value.revision=revid;
							versions.put(id, value);
							newrevisions++;
						}else{
							equalfiles++;
							newrev=false;
						}


					}

					versionslock.unlock();

					if(newrev){
						sendData( id, value.version, output.toByteArray() );
					}
					//System.out.println("id "+ id + " version "+ value.version + " revisions "  + value.revision );
					inPage = false;
					gotId = false;
					gotRevId = false;
					newrev=true;
					output.reset();
				}
			}
			//System.out.println("End Read file "+file.getName()+ " new files " + newfiles + " new revisions " + newrevisions + " equal files " + equalfiles );
			reader.close();
		}
		catch ( IOException ioe )
		{
			ioe.printStackTrace();
		}
	}

	/**
	 * Send article's content to server.
	 *
	 * @param name    Article's name
	 * @param version Article's version
	 * @param content Article's content
	 */
	public static void sendData( String name, int version, byte[] content )
	{
		dfcli.put(Long.parseLong(name), new Long(version), content);
	}


}

class Wthread implements Runnable{
	
	public Wthread(){
	}
	public void run(){

		WikiPutter.fileslock.lock();
		String pstr = WikiPutter.files.pollFirst();
		WikiPutter.fileslock.unlock();
		while(pstr!=null){
			System.out.println("[WIKILOAD] Processing: "+pstr);
			File file = new File(pstr);
			WikiPutter.parseFile(file);	
			WikiPutter.fileslock.lock();
			pstr = WikiPutter.files.pollFirst();
			WikiPutter.fileslock.unlock();
		}
	}
}


class Wvalue {

	public int version;
	public String revision;

	Wvalue(int v, String r){

		this.version=v;
		this.revision=r;
	}



}





