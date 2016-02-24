package ca.ualberta.library.openstack;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
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
public class ERAModify {

	private String directory;
	private static final Log log = LogFactory.getLog(ERAModify.class);
    private HttpClient client;
    private String url;
    private String eraDatabase;
    private String className;
    private String userName;
    private String password;
    private String databaseURL;
	Connection connection = null;

 	public static void main(String[] args) {
//    public void execute() throws BuildException {
    	
    	String path;
    	String fileType = null;
  	
    	FileUtils folder = new FileUtils();
    	ERAModify migrate = new ERAModify();
    	JCloudSwift jcloud = new JCloudSwift();
    	
    	migrate.init();

    	try {
                        /* Added by Henry Zhang on February 19, 2016 
                         * 
                         */
                        DateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
                        Date date=new Date();
                        dateDir=dateFormat.format(date));
                        log.info("Ingest objects in file modify/era-modify_"+dateDir+".txt");
			FileInputStream fstream = new FileInputStream("modify/era-modify_" + dateDir + ".txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null)   {
				migrate.saveObject(strLine);
				
		    	File directory = new File(strLine);
		    	if (directory.exists()) {
					log.info("File read: " + directory.getName());
					
		    		String filename = directory.getName();
		    		path = directory.getAbsolutePath();
		    		
	    			if (path.contains("\\objects\\")) {
						jcloud.deleteObject("era/objects", directory.getName());
						jcloud.uploadObjectFromFile("era/objects", directory);
					}
					else {
						jcloud.deleteObject("era/datastreams", directory.getName());
						jcloud.uploadObjectFromFile("era/datastreams", directory);
					}
	    			
					if (path.contains("\\objects\\")) {
						MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/objects", filename);
					}
					else {
						MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/datastreams", filename);
					}
					
					migrate.saveObject(strLine);
					
					log.info("File processed: " + directory.getName());
		    	}
			}	
	    }	
		catch (FileNotFoundException e) {
			log.error(e.getMessage());
		}  
		catch (IOException e) {
			log.error(e.getMessage());
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
					fileFound = false;
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
