package ca.ualberta.library.openstack;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Date;


import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;

import ca.ualberta.library.jclouds.JCloudSwift;

public class ERAInfo {

	private String directory;
	private static final Log log = LogFactory.getLog(ERAInfo.class);
	private HttpClient client;
	private String url;
	private String eraDatabase;
	private String className;
	private String userName;
	private String password;
	private String databaseURL;

 	public static void main(String[] args) {
 		
		String path;
		String fileType = null;
		Writer writer = null;
		
		FileUtils folder = new FileUtils();
		ERAInfo info = new ERAInfo();
		JCloudSwift jcloud = new JCloudSwift();
		
		info.init();
		
		String DATE_FORMAT  = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);   
		
		try {
		    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("missing/modified-dates.txt"), "utf-8"));
			
			FileInputStream fstream = new FileInputStream("era.txt");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null)   {
		    	String file = strLine.substring(0, strLine.indexOf(","));
		    	file = file.replace(":", "_");
				
				log.info("File read: " + file);
				
				MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era/data/objects", file);
				if (metadata != null) {
					Date lastModifiedDt = metadata.getLastModified();
					String strLastModifiedDt = sdf.format(lastModifiedDt);
				
					log.info("Date: " + strLastModifiedDt);
				
					writer.write(file + "," + strLastModifiedDt);
				}	
				
				if (jcloud.doesObjectExist("era", "data/objects/" + file)) {
					jcloud.getObject("era", "data/objects/" + file, "missing/");
				}
				else {
					log.info("File missing:" + file);
				}
				
				log.info("File processed: " + file);
	    	}
			
			writer.close();
	    }	
		catch (FileNotFoundException e) {
			log.error(e.getMessage());
		}  
		catch (IOException e) {
			log.error(e.getMessage());
		}  
	
 	}


 	public void init() {
	
 		client = new HttpClient();
 		client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
	
 	}
		
}
