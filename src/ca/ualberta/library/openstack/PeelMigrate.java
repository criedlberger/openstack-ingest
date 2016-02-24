package ca.ualberta.library.openstack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//public class PeelMigrate extends Task {
public class PeelMigrate {

	private String directory;
	private static final Log log = LogFactory.getLog(PeelMigrate.class);
	
	public static void main(String[] args) {
//    public void execute() throws BuildException {
    	
    	String filename;
    	String filePrefix;
    	int stringIndex;
    	
    	File folder = new File("sers");
    	File[] listOfFiles = folder.listFiles(); 
    			   		   
    	for (int i = 0; i < listOfFiles.length; i++) {
    		if (listOfFiles[i].isFile()) {
	    		filename = listOfFiles[i].getName();
	    		stringIndex = filename.lastIndexOf(".pdf");
	    		if (stringIndex > -1) {
	    			filePrefix = filename.substring(0, stringIndex);
	    		}
	    		else {
	    			filePrefix = filename;
	    		}
	    	
	    		try {
	    			File inputFile = new File("sers" + "/" + filename);
	    			long time = inputFile.lastModified();
	    			FileInputStream inputStream = new FileInputStream(new File("sers" + "/" + filename));
	    		
		    		filename = filename.replaceAll(",", "");
		    		
    				boolean digitFound = false;
    				stringIndex = 0;
    				int digitCount = 0;
    				while (stringIndex < filename.length() && !digitFound) {
    					if (Character.isDigit(filename.charAt(stringIndex))) {
    						digitFound = true;
    						
    						if (!Character.isDigit(filename.charAt(stringIndex + 1))) {
    							filename = filename.substring(0, stringIndex) + "00" + filename.substring(stringIndex, filename.length()); 
    						}
    						else
    						{
    							if (!Character.isDigit(filename.charAt(stringIndex + 2))) {
    								filename = filename.substring(0, stringIndex) + "0" + filename.substring(stringIndex, filename.length());
    							}	
    						
    						}
    					}
    					
    					stringIndex++;
    				}
    				
    				filename = filename.substring(0,6);
    				
 //   				String directoryPath = "test/" + filename.substring(0, 2) + "/" + filename.substring(2, 4) + "/" + filename.substring(4, 6) + "/" + filename;
    				String directoryPath = "test/";
		    				
					File dir = new File(directoryPath);
					if (!dir.exists()) {
						dir.mkdirs();
					}	
					
    				filename = filename.substring(0,6) + ".pdf";
    				
					FileOutputStream output = new FileOutputStream(new File(directoryPath + "/" + filename));
    		
			    	try {
			    		pipeStream(inputStream, output, 4096);
			    	}	
					catch (IOException e) {
						System.out.println(e.getMessage());
			        }
					
					File outputFile = new File(directoryPath + "/" + filename);
					outputFile.setLastModified(time);
					
		        	System.out.println(filename.toString());
	    		}
	    		catch (FileNotFoundException e) {
	    			System.out.println(e.getMessage());

	    		}  
    		}	
        }
    }
    
    public void setDirectory(String directory) {
        this.directory = directory;
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
