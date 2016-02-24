package ca.ualberta.library.openstack;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Collection;
import java.util.Date;
import java.text.ParseException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;

import ca.ualberta.library.jclouds.JCloudSwift;

public class ERATarSearch {

		private String directory;
		private static final Log log = LogFactory.getLog(ERATarSearch.class);
	    private HttpClient client;
		Connection connection = null;

	 	public static void main(String[] args) {
	    	
	    	String path;
	    	String fileType = null;
	  	
	    	FileUtils folder = new FileUtils();
	    	ERATarSearch search = new ERATarSearch();
	    	
	    	search.init();
	    	
	    	File directory = new File("/pseudoHoneycomb");
	    	Collection<File> fileList = folder.listFiles(directory, null, true);

	    	for (File file : fileList) {
	    		String name = file.getName();
	    		
    			String DATE_FORMAT  = "yyyy-MM-dd";
				SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT); 
				
	    		long lastModified = file.lastModified();
	    		Date lastModifiedDt = new Date(lastModified);
	    		String lastModifiedStr = sdf.format(lastModifiedDt.getTime());
	    				
	    		log.info("File: " + file.getPath() + ": " + lastModifiedStr);
	    		
	    		if (!file.getPath().equals("/pseudoHoneycomb/d21/d135/f75")) {
		    		try {
		    			Date startDt = new SimpleDateFormat(DATE_FORMAT).parse("2013-12-11");
					
			    		if (file.isFile()  && name.startsWith("f", 0)) {
			    			if (lastModifiedDt.after(startDt)) {
			    				search.getFiles(file.getPath());
			    			}	
		    			}
		    		}
		    		catch (ParseException e) {
		    			e.getMessage();
		    		}
	    		}	
		   	}
	    	
	    }
	    
	    public void init() {
	    	
		  	client = new HttpClient();
	    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
	    	
	    }
	    
	    public void getFiles(String file) {
	    	
	    	String fileType = null;
	   	 	String filename = null;
	   	 	File upload = null;
	     	
	        TarArchiveInputStream tarIn = null;

	        try {
		        tarIn = new TarArchiveInputStream((new BufferedInputStream(new FileInputStream(file))));
		        
		        TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
		        while (tarEntry != null) {
		        	String tarFile = tarEntry.getName(); 
		        	
	    			log.info("File read: " + tarFile);
		        	
//	    			String test = "/datastreams/2013/1230/";
//	   				if (test.matches(".*/datastreams/2013/121[2-9]/.*") || test.matches(".*/datastreams/2013/122[0-9]/.*") || test.matches(".*/datastreams/2013/123[0-1]/.*")) {
//	    				log.info("found");
//	    			}
	    			
		        	if ((tarFile.contains("production/fedora/data/datastreams/") && 
		        	  	(tarFile.matches(".*/2013/121[2-9]/.*") ||
		        	  	 tarFile.matches(".*/2013/122[0-9]/.*") ||
		        	  	 tarFile.matches(".*/2013/123[0-1]/.*") ||
		        	  	 tarFile.matches(".*/2014/010[1-9]/.*") ||
		        	  	 tarFile.matches(".*/2013/011[1-4]/.*"))) ||
			        	(tarFile.contains("production/fedora/data/objects/") &&		        			
			            (tarFile.matches(".*/2013/121[2-9]/.*") ||
			             tarFile.matches(".*/2013/122[0-9]/.*") ||
			        	 tarFile.matches(".*/2013/123[0-1]/.*") ||
			        	 tarFile.matches(".*/2014/010[1-9]/.*") ||
			        	 tarFile.matches(".*/2013/011[1-4]/.*"))) &&
		        		tarFile.contains("uuid_")) {
			            File destPath = new File(tarEntry.getName());
			            Date lastModifiedDt = tarEntry.getLastModifiedDate();
			            
		                File outputFile = new File("missing/" + destPath.getName());
		                
		                int index = 0;
		                while (outputFile.exists()) {
		                	index++;
		                	outputFile = new File("missing/" + destPath.getName() + "-" + Integer.toString(index));
		                	log.info("Duplicate: " + outputFile.getName());
		                }
		                
		                byte [] btoRead = new byte[1024];
		                BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream("missing/" + outputFile.getName()));
		                int len = 0;
		
		                while((len = tarIn.read(btoRead)) != -1)
		                {
		                    bout.write(btoRead,0,len);
		                }
		                
		                
		                bout.close();
		                btoRead = null;
		                
		                outputFile.setLastModified(lastModifiedDt.getTime());
		                
		    			log.info("File copied: " + destPath.getName());
		        	}    
	                
		        	tarEntry = tarIn.getNextTarEntry();
			    }
		        
			    tarIn.close();
			}
		    catch (IOException e) {
		       	log.info(e.getMessage());
		    }
	    }
	   	
		public static void pipeStream(InputStream in, FileOutputStream out, int bufSize)
		throws IOException {
			try {
					byte[] buf = new byte[bufSize];
					int len;
					while ( ( len = in.read( buf ) ) > 0 ) {
						out.write( buf, 0, len );
				}
			} finally {
				try {
					in.close();
					out.close();
				} catch (IOException e) {
					System.out.println("WARNING: Could not close stream.");
				}
			}
		}
}
