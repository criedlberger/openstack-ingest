package ca.ualberta.library.openstack;

import ca.ualberta.library.jclouds.JCloudSwift;

public class JCloudTest {

 	public static void main(String[] args) {
    	JCloudSwift jcloud = new JCloudSwift();
    	jcloud.listContainers();
 	}
}
