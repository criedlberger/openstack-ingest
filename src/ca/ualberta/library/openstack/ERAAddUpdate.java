package ca.ualberta.library.openstack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Date;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;
import java.io.OutputStream;
import java.io.FileOutputStream;

import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;

import ca.ualberta.library.jclouds.JCloudSwift;

//public class CihmMigrate extends Task {
public class ERAAddUpdate {

	private String directory;
	private static final Log log = LogFactory.getLog(ERAAddUpdate.class);
    private HttpClient client;
    private String url;
    private String eraDatabase;
    private String className;
    private String userName;
    private String password;
    private String databaseURL;
    String startDate;
    String days;
    String sourceDir;

	Connection connection = null;

 	public static void main(String[] args) {
//    public void execute() throws BuildException {
    	
    	String path;
    	String fileType = null;
  	
    	FileUtils folder = new FileUtils();
    	ERAAddUpdate migrate = new ERAAddUpdate();
    	JCloudSwift jcloud = new JCloudSwift();
    	
    	migrate.init();
    	
	String DATE_FORMAT  = "yyyy/MMdd";
	SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);   
	Date start = sdf.format(startDate);
	Calendar currentDate = Calendar.setTime(start);
	    
    	String strDate = null;
	    for (int index = 1; index <= Integer.parseInt(days); index++) {
	    	strDate = Integer.toString(index);
		    if (strDate.length() == 1) {
		    	strDate = "0" + strDate;
		    }	
		
		String dateStr = sdf.format(currentDate.getTime());
    
		    log.info(dateStr);
		    
			log.info("Processing ERA objects");
		
                #Commented out by Chris and Henry on Feburary 18, 2016 because of the path problem
                #(beginning with /mnt which is not right)	
	    	#File directory = new File("/mnt/era/production/fedora/data/objects/" + dateStr);
	    	File directory = new File(sourceDir + "objects/" + dateStr);
	    	
	    	if (directory.exists()) {
		    	Collection<File> fileList = folder.listFiles(directory, null, true);
		
		    	for (File file : fileList) {
		    		if (file.isFile() && file.exists()) {
						
						log.info("File read: " + file.getName());
						
			    		String filename = file.getName();
			    		path = file.getAbsolutePath();
			    		try {
			    			if (path.contains("/objects/")) {
								jcloud.deleteObject("era/objects", file.getName());
								jcloud.uploadObjectFromFile("era/objects", file);
							}
							else {
								jcloud.deleteObject("era/datastreams", file.getName());
								jcloud.uploadObjectFromFile("era/datastreams", file);
							}
			    			
		    			
							if (path.contains("/objects/")) {
								MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/objects", filename);
							}
							else {
								MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/datastreams", filename);
							}
							
							migrate.saveObject(path);
							
							log.info("File processed: " + file.getName());
			    		}
			    		catch (FileNotFoundException e) {
			    			log.error(e.getMessage());
			    		}  
			    		catch (IOException e) {
			    			log.error(e.getMessage());
			    		}  
		    		}	
		        }
	    	}	
	    	
			log.info("ERA objects processed");
			
			log.info("Processing ERA datastreams");
			
#	    	directory = new File("/mnt/era/production/fedora/data/datastreams/" + strDate);
	    	directory = new File( sourceDir + "/datastreams/" + strDate);
	    	if (directory.exists()) {
		    	Collection<File> fileList = folder.listFiles(directory, null, true);
		
		    	for (File file : fileList) {
		    		if (file.isFile() && file.exists()) {
						
						log.info("File read: " + file.getName());
						
			    		String filename = file.getName();
			    		path = file.getAbsolutePath();
			    		try {
			    			if (path.contains("/objects/")) {
								jcloud.deleteObject("era/objects", file.getName());
								jcloud.uploadObjectFromFile("era/objects", file);
							}
							else {
								jcloud.deleteObject("era/datastreams", file.getName());
								jcloud.uploadObjectFromFile("era/datastreams", file);
							}
			    			
		    			
							if (path.contains("/objects/")) {
								MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/objects", filename);
							}
							else {
								MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/datastreams", filename);
							}
							
							migrate.saveObject(path);
							
							log.info("File processed: " + file.getName());
			    		}
			    		catch (FileNotFoundException e) {
			    			log.error(e.getMessage());
			    		}  
			    		catch (IOException e) {
			    			log.error(e.getMessage());
			    		}  
		    		}	
		        }
	    	}

                log.info("ERA datastreams processed");

                currentDate.add(Calendar.DATE, 1);
	    }	
	    	
		
    }
    
    public void setDirectory(String directory) {
        this.directory = directory;
    }
    
    public void init() {
    	
		try
		{
	    	Properties sqlProperties = new Properties();
		  	try {
		  		sqlProperties.load(new FileInputStream("sql.properties"));
			  
			  	className = sqlProperties.getProperty("className");
			  	userName = sqlProperties.getProperty("userName");
			  	password = sqlProperties.getProperty("password");
			  	databaseURL = sqlProperties.getProperty("databaseURL");
			  	eraDatabase = sqlProperties.getProperty("eraDatabase");
		  	}	  
		  	catch (IOException e) {
			  	e.printStackTrace();
		  	}
	    	
			Class.forName (className).newInstance();
			connection = DriverManager.getConnection (databaseURL, userName, password);
		 	log.info("Database connection established");
	    }
	    catch (Exception e)
	    {
	        log.error("Cannot connect to database server");
	        log.error(e.getMessage());
	    }
	

    	Properties addProperties = new Properties();
	  	try {
	  		addProperties.load(new FileInputStream("add.properties"));
		  
		  	startDate = addProperties.getProperty("startDate");
		  	days = addProperties.getProperty("days");
                        sourceDir  = addProperties.getProperty("source_directory"); 
		  	}	  
		catch (IOException e) {
		  	e.printStackTrace();
		}


	
	  	client = new HttpClient();
    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
    	
    }
   	
    public void saveObject(String file) {
    	
    	boolean fileFound = false;
    	String uuid = null;
    	String datastreamId = "";
 
    	int lastIndex = file.lastIndexOf("/");
    	if (lastIndex > -1) {
    		uuid = file.substring(lastIndex + 1);
    	}
    	
    	if (file.contains("/datastreams/")) {
        	int firstIndex = uuid.indexOf("+");
        	if (firstIndex > -1) {
        		uuid = uuid.substring(0, firstIndex); 
        	}			
			
	    	lastIndex = file.lastIndexOf("+");
	    	if (lastIndex > -1) {
	    		datastreamId = file.substring(lastIndex + 1);
	    	}
       	}
		uuid = "\"" + uuid + "\"";
		datastreamId = "\"" + datastreamId + "\"";
    		
    	
		if (connection != null)
		{
			 try {
				Statement statement = connection.createStatement();
					
				file = "\"" + file + "\"";
				String fetch = "SELECT * FROM " + eraDatabase + " WHERE FILE = " + file;

				statement.executeQuery(fetch);

				ResultSet resultSet = statement.getResultSet();
				while (resultSet.next())
				{
					fileFound = true;
				}
				
				if (!fileFound) {
					Statement statementInsert = connection.createStatement();
	
					String update = "INSERT INTO " + eraDatabase + " (file, uuid, datastream_id) VALUES(" + file + "," + uuid + "," + datastreamId + ")";
	
					statementInsert.executeUpdate(update);
					
					statementInsert.close();
				}
				
				statement.close();
			}
			catch (SQLException e) {
				log.error("SQL Exception: " + e.getMessage() + " for file: " + file);
			}
		}	
    }
    
}
