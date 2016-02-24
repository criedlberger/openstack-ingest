package ca.ualberta.library.openstack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jclouds.openstack.swift.domain.MutableObjectInfoWithMetadata;

import se.kb.oai.pmh.Header;

import ca.ualberta.library.jclouds.JCloudSwift;

public class ERACopy {

	private String directory;
	private static final Log log = LogFactory.getLog(ERACopy.class);
    private HttpClient client;
    private String url;
    private String eraDatabase;
    private String className;
    private String userName;
    private String password;
    private String databaseURL;
	Connection connection = null;

 	public static void main(String[] args) {
    	
    	String path;
    	String fileType = null;
    	List<String> fileList = new ArrayList<String>();
  	
    	FileUtils folder = new FileUtils();
    	ERACopy migrate = new ERACopy();
    	JCloudSwift jcloud = new JCloudSwift();
    	
    	migrate.init();
    	
    	fileList = jcloud.listObjectsWithFiltering("era", "data/");
    	
		
		HashMap<String, String> metadata = new HashMap<String, String>();
		metadata.put("contentType", "application/octet-stream");
		
		log.info("List:" + fileList.size());
		
		for (String file : fileList) {
			
/*			MutableObjectInfoWithMetadata metadata = jcloud.getObjectInfo("era", file);
			String type = metadata.getContentType();
			log.info(type);*/
			
			String newFile = file.replace("data/", "");
			jcloud.copyObject("era", file, "era", newFile);
			jcloud.setObjectInfo("era", newFile, metadata);
			
			log.info(file);
			log.info(newFile);
		}	
    }
    
    public void init() {
    	
	  	client = new HttpClient();
    	client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);
    	
    }
   	
}
