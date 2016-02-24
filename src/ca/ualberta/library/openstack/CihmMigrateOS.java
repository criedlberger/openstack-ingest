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
import java.util.ArrayList;
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
public class CihmMigrateOS {

	private String directory;
	private static final Log log = LogFactory.getLog(CihmMigrateOS.class);
    private HttpClient client;
    private String url;
    private String cihmDatabase;
    private String noidDatabase;
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
    	CihmMigrateOS migrate = new CihmMigrateOS();
    	
    	migrate.init();
    	
 //    	File directory = new File("Z:\\cihm\\ci\\hm\\_1\\01");
//    	Collection<File> fileList = folder.listFiles(directory, null, true);

//		jcloud.init();
//    	try {
			
			migrate.getFiles();
			
/*	    	for (File file : fileList) {
	    		if (file.isFile()) {
		    		filename = file.getName();
		    		path = file.getAbsolutePath();
		    		try {
		    			String noid = migrate.mintNoid();
		    			jcloud.uploadObjectFromFile("test", file);
		    			
		    			MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("test", file.getName());
		    			
		    			int index = filename.lastIndexOf("_");
		    			if (index > -1) {
		    				fileType = filename.substring(index+1, filename.lastIndexOf("."));
		    				filename = filename.substring(0, index);
		    			}
		    			
		    			String identifier = file.getName();
		    			index = identifier.lastIndexOf(".");
		    			if (index > -1) {
		    				identifier = identifier.substring(0, index);
		    			}
		    			
		    			long fileSize = metadata.getBytes();
		    			if (fileSize == migrate.checkSize(filename, fileType)) {				
		    				migrate.saveIdentifier(identifier, noid);
		    				
		    				System.out.println(identifier.toString());
		    			}
		    			else {
				        	System.out.println("File size does not match for: " + filename.toString());
		    			}	
	    		}
		    		catch (FileNotFoundException e) {
		    			System.out.println(e.getMessage());
		    		}  
		    		catch (IOException e) {
		    			System.out.println(e.getMessage());
		    		}  
	    		}	
	        }*/
/*    	}
    	catch (IOException e) {
    		e.getStackTrace();
    	}*/
    	
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
			  	cihmDatabase = sqlProperties.getProperty("cihmDatabase");
			  	noidDatabase = sqlProperties.getProperty("noidDatabase");
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
		
    	Properties noidProperties = new Properties();
	  	try {
	  		noidProperties.load(new FileInputStream("noid.properties"));
		  
		  	url = noidProperties.getProperty("url");
	  	}	  
	  	catch (IOException e) {
		  	e.printStackTrace();
	  	}
    	
	  	client = new HttpClient();
    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
    	
    }
    
    public void getFiles() {
    	
    	String fileType = null;
   	 	String filename = null;
   	 	File upload = null;
    	JCloudSwift jcloud = new JCloudSwift();
     	
//		jcloud.deleteContainer("test");
//		jcloud.createContainer("test");
		
		if (connection != null)
		{
			 try {
				Statement statement = connection.createStatement();
				Statement statementCheck = connection.createStatement();

				String field = "\"" + "%_marc.xml%" + "\"";
				String fetch = "SELECT * FROM " + noidDatabase + " WHERE FILE LIKE " + field + " AND noid = \"\" AND disk like \"d%\"";

				statement.executeQuery(fetch);

				ResultSet resultSet = statement.getResultSet();
				while (resultSet.next())
				{
					String file = resultSet.getString("file");
					String disk  = resultSet.getString("disk");
					String noidSQL = resultSet.getString("noid");
					
			        TarArchiveInputStream tarIn = null;

			        try {
				        tarIn = new TarArchiveInputStream((new BufferedInputStream(new FileInputStream("/pseudoHoneycomb/" + disk))));
				        
				        boolean fileFound = false;
				        TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
				        while (tarEntry != null && !fileFound) {
				        	if (file.equals("Z:/" + tarEntry.getName())) {
				        		fileFound = true;
					            File destPath = new File(tarEntry.getName());
					            
				                byte [] btoRead = new byte[1024];
				                BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(destPath.getName()));
				                int len = 0;
				
				                while((len = tarIn.read(btoRead)) != -1)
				                {
				                    bout.write(btoRead,0,len);
				                }
				
				                bout.close();
				                btoRead = null;
				                upload = new File(destPath.getName());
				                
				    			String identifier = upload.getName();
				    			int index = identifier.lastIndexOf("_marc.");
				    			if (index > -1) {
				    				identifier = identifier.substring(0, index);
				    			}
				    			
//								String checkFile = file.replace("_abbyy.zip", ".pdf");
								String check = "SELECT * FROM noid_cihm_unique WHERE identifier = " + "\"" + identifier + "\"";;
									
								statementCheck.executeQuery(check);

								ResultSet checkSet = statementCheck.getResultSet();
								while (checkSet.next())
								{
									noidSQL = checkSet.getString("noid");
								}
								
                                String noid = null;
							  	if (noidSQL.isEmpty()) {
									noid = mintNoid();
							  	}
							  	else {
									noid = noidSQL;
								}
				    			
				    			jcloud.uploadObjectFromFile("cihm/" + noid + "/marc/1", upload);
				    			
				    			index = file.lastIndexOf("/");
				    			if (index > -1) {
				    				filename = file.substring(index+1);
				    				fileType = filename.substring(filename.lastIndexOf(".")+1);
				    			}
				    			
				    			MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("cihm/" + noid +"/marc/1", filename);
			    			
				    			identifier = filename;
				    			index = identifier.lastIndexOf("_marc.xml");
				    			if (index > -1) {
				    				identifier = identifier.substring(0, index);
				    			}
				    			
				    			
				    			long fileSize = metadata.getBytes();
				    			if (fileSize == checkSize(identifier, "marc")) {				
				    				System.out.println(identifier.toString());
				    			}
				    			else {
						        	System.out.println("File size does not match for: " + filename.toString());
				    			}
				    			
			    				saveIdentifier(file, noid);
			    				
				    			log.info("File copied: " + filename);
				        	}    
			                
				        	tarEntry = tarIn.getNextTarEntry();
					    }
				        
					    tarIn.close();
		                upload.delete();
					}
				    catch (IOException e) {
				       	log.info(e.getMessage());
				    }
				}
				
				statementCheck.close();
				statement.close();
			}
			catch (SQLException e) {
				log.error("SQL Exception: " + e.getMessage());
			}
		    	
			jcloud.listObjects("cihm");
			
			try {
				jcloud.close();
			}	
    		catch (IOException e) {
    			System.out.println(e.getMessage());
    		}  
		}	
    }
   	
    public String mintNoid() {
   	
    	HttpMethod method = new PostMethod(url);
    	
    	String responseBody = null;
    	String noid = null;
    	try{
             client.executeMethod(method);
             responseBody = method.getResponseBodyAsString();
	         noid = responseBody.substring(responseBody.indexOf(":")+2,responseBody.indexOf("\n"));
	         log.error(noid);
        } 
    	catch (HttpException he) {
        	 log.error("Http error connecting to '" + url + "'");
        	 log.error(he.getMessage());
        } 
    	catch (IOException e){
             e.getStackTrace();
        }     
    	
    	return noid;
    }
    
    public void saveIdentifier(String file, String noid) {
    	
		if (connection != null)
		{
			 try {
				Statement statementUpdate = connection.createStatement();

				file = (String) "\"" + file + "\"";
				noid = (String) "\"" + noid + "\"";
				String update = "UPDATE " + noidDatabase + " SET noid=" + noid + " WHERE file=" + file;

				statementUpdate.executeUpdate(update);

				statementUpdate.close();
			}
			catch (SQLException e) {
				log.error("SQL Exception: " + e.getMessage() + " for file: " + file);
			}
		}	
    }
   	
    public long checkSize(String identifier, String fileType) {
    	
    	String fileSize = "0";
    	String fileTypeText = null;
    	
    	if (fileType.equals("pdf")) {
    		fileTypeText = "pdf_txt"; 
    	}
    	
		if (connection != null)
		{
			try {
				Statement statement = connection.createStatement();

				identifier = "\"" + identifier + "\"";
				String query = "SELECT * FROM " + cihmDatabase + " where identifier = " + identifier;

				statement.executeQuery(query);

				ResultSet resultSet = statement.getResultSet();
				while (resultSet.next())
				{
					fileSize = resultSet.getString(fileType);
					if (fileType.equals("pdf") && fileSize.equals("0")) {
						fileSize = resultSet.getString(fileTypeText);
					}
				} 
			}	
			catch (SQLException e) {
				log.error("SQL Exception: " + e.getMessage() + " for file: " + identifier);
			}
		}	
		
		return Long.parseLong(fileSize);
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
