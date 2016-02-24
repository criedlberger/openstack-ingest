package ca.ualberta.library.openstack;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;
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
import java.util.Properties;

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

public class ERAMigrate {

	private String directory;
	private static final Log log = LogFactory.getLog(ERAMigrate.class);
    private HttpClient client;
    private String url;
    private String eraDatabase;
    private String className;
    private String userName;
    private String password;
    private String databaseURL;
	Connection connection = null;
        String sourceDir;

 	public static void main(String[] args) {
//    public void execute() throws BuildException {
    	
    	String path;
    	String fileType = null;
  	
    	FileUtils folder = new FileUtils();
    	ERAMigrate migrate = new ERAMigrate();
    	
    	migrate.init();
		migrate.getFiles();
			
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
    
    public void getFiles() {
    	
    	String fileType = null;
    	JCloudSwift jcloud = new JCloudSwift();
     	
//		jcloud.deleteContainer("era");
//		jcloud.createContainer("era");
//		jcloud.getContainerMetadata("era");
//		jcloud.listObjects("era");
		
		
		if (connection != null)
		{
			 try {
				Statement statement = connection.createStatement();

				String field = "\"" + "/mnt/%" + "\"";
//				String field = "\"" + "%uuid_7078db8f-e2fc-47be-b29b-9656a914cd0e+DS1+DS1.0%" + "\"";
				String fetch = "SELECT * FROM " + eraDatabase + " WHERE FILE LIKE " + field;
//				String fetch = "SELECT * FROM " + eraDatabase + " WHERE FILE=\"W:/production/fedora/data/datastreams/2013/0903/14/34/uuid_446a0dcd-3f6c-4005-8ffa-982820c6725b+DS1+DS1.0\"";
//				String fetch = "SELECT * FROM era_files_copy";
				statement.executeQuery(fetch);

				ResultSet resultSet = statement.getResultSet();
				while (resultSet.next())
				{
					String file = resultSet.getString("file");
//					String size = resultSet.getString("size");
					String copy1 = resultSet.getString("copy_1");
					String copy4 = resultSet.getString("copy_4");
//					String modifiedDtstr = resultSet.getString("modified_dt");
//					String changedDtstr = resultSet.getString("changed_dt");

					log.info(file);
										
/*					String disk = null;
					
					if (copy4 != null) {
						if (copy4.startsWith("d") && copy4.contains("/")) {
							disk = copy4;
						}
					}	
					
					if (disk == null) {
						if (copy1.startsWith("d") && copy1.contains("/")) {
							disk = copy1;
						}
					}

					log.info(disk);*/
					
					Date modifiedDt = null;
					Date changedDt = null;
/*					try {
		    			String DATE_FORMAT  = "MMM  d  yyyy";
						SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT); 
						
						if (modifiedDtstr != null) {
							modifiedDtstr = modifiedDtstr.trim();
							modifiedDt = sdf.parse(modifiedDtstr);
						}
						
						if (changedDtstr != null) {
							changedDtstr = changedDtstr.trim();
							changedDt = sdf.parse(changedDtstr);
						}	
					} catch (ParseException e) {
						log.error(e.getMessage());
					}*/
					
					String filename = null;
//					if (disk != null) {
						int lastIndex = file.lastIndexOf("/");
						if (lastIndex > -1) {
							filename = file.substring(lastIndex + 1);
						}
						
						log.info("Processing: " + filename);
						
/*				        TarArchiveInputStream tarIn = null;
				        try {
					        tarIn = new TarArchiveInputStream((new BufferedInputStream(new FileInputStream("/pseudoHoneycomb/" + disk))));
					        
					        boolean fileFound = false;
					        TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
					        while (tarEntry != null && !fileFound) {
					        	if (file.equals(tarEntry.getName())) {
					        		fileFound = true;*/
					        		
//									File upload = new File(filename);
									File upload = new File(file);
									
/*					                byte [] btoRead = new byte[1024];
					                BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(upload.getName()));
					                int len = 0;
					
					                while((len = tarIn.read(btoRead)) != -1)
					                {
					                    bout.write(btoRead,0,len);
					                }
					
					                bout.close();
					                btoRead = null;*/
					                
					                log.info("Copying file: " + filename);
					                
					                try {
										if (file.contains("/objects/")) {
											jcloud.uploadObjectFromFile("era/objects", upload);
										}
										else {
			//								jcloud.deleteObject("era/data/datastreams", upload.getName());
											jcloud.uploadObjectFromFile("era/datastreams", upload);
										}
					                }
						    		catch (IOException e) {
						    			System.out.println(e.getMessage());
						    		}
					                
					    			
					    			int index = file.lastIndexOf("/");
					    			if (index > -1) {
					    				filename = file.substring(index+1);
					    				fileType = filename.substring(filename.lastIndexOf(".")+1);
					    			}
					    			
					    			log.info("Retrieving metadata for file: " + filename);
					    			
					    			MutableObjectInfoWithMetadata metadata = null;
					    			if (!filename.equals("uuid_97278e43-23f7-44ee-9cff-67a60655a48b+THUMBNAIL+THUMBNAIL.0")) {
										if (file.contains("/objects/")) {
											metadata = jcloud.getObjectInfo("era/objects", filename);
										}
										else {
											metadata = jcloud.getObjectInfo("era/datastreams", filename);
										}
				    			
										if (metadata != null) {
											log.info("metadata");
											long fileSize = metadata.getBytes();
											Date date = metadata.getLastModified();
											
/*											if (fileSize != Integer.parseInt(size)) {
												log.error("File size not equal for: " + filename);
											}*/
										}
					    			}	
									
					    			String identifier = filename;
					    			index = identifier.lastIndexOf(".");
					    			if (index > -1) {
					    				identifier = identifier.substring(0, index);
					    			}
					    			
/*					    			if (upload != null) {
					    				upload.delete();
					    			}*/	
					    		/*}
					        	
					        	tarEntry = tarIn.getNextTarEntry();
					       	}
					       						        
//						    tarIn.close();
						}    
			    		catch (IOException e) {
			    			System.out.println(e.getMessage());
			    		}
					}*/    
				}
				
				statement.close();
			}
			catch (SQLException e) {
				log.error("SQL Exception: " + e.getMessage());
			}
		    	
//			jcloud.listObjects("era");
			
			try {
				jcloud.close();
			}	
    		catch (IOException e) {
    			System.out.println(e.getMessage());
    		}  
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
