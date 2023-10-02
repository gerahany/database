package database2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JTable;






public class table implements Serializable{
	
	Vector<String> indexpath=new Vector<String>();
	Hashtable <Integer,Integer> infopage;
	Vector<String> pagesbucket;
	String tablepath;
	public String getTablepath() {
		return tablepath;
	}

	public void setTablepath(String tablepath) {
		this.tablepath = tablepath;
	}

	String strTableName;
	String strClusteringKeyColumn;
	Vector<String> pagespaths= new Vector<String>()  ;
	Hashtable<String,String> htblColNameType;
	Hashtable<String,String> htblColNameMin;
	Hashtable<String,String> htblColNameMax ;
	
	

    private String pathfolder;

	public table(String strTableName,String strClusteringKeyColumn
			,Hashtable<String,String> htblColNameType,Hashtable<String,String> htblColNameMin,Hashtable<String,String> htblColNameMax
			) {
		this.strTableName=strTableName;
		this.strClusteringKeyColumn=strClusteringKeyColumn;
//		this.htblColNameType=htblColNameType;
//		this.htblColNameMin=htblColNameMin;
//		this.htblColNameMax=htblColNameMax;
		this.pagesbucket=new Vector<String>();
		infopage=new Hashtable <Integer,Integer>();
		this.indexpath=new Vector<String>();
		

     
//        try {
//            this.pathToPages = Paths.get(Resources.getResourcePath(), this.strTableName).toString();
//        } catch (IOException | URISyntaxException e) {
//            e.printStackTrace();
//            System.out.println("[ERROR] something wrong habben when trying to read the resources location");
//        }
//
//        if (Files.exists(Paths.get(pathToPages))) {
//            // it throws an Error as the relation with that name aleardy Exists
//            System.out.println("[ERROR] table already exists");
//            throw new DBAppException();
//        } else {
//            try {
//                Files.createDirectories(Paths.get(pathToPages));
//            } catch (IOException e) {
//                e.printStackTrace();
//                System.out.println("[ERROR] something wrong habben when trying to make the directory for the pages");
//            }
//        }

        // TODO write this info to the csv also

        // save the table object to the disk
		
	}

	 public page searchforPage(tuple tuple) {
	        int index = 0;
	        int max = pagesbucket.size() - 1;
	        int min = 0;

	        while (max >= min) {
	            int i = (max + min) / 2;
	            
	            page page =pagedeserialize( pagesbucket.get(i));
	            if (((Comparable) page.min).compareTo(tuple.getPrimkey()) <= 0) {
	                min = i + 1;
	                index = i;
	            } else {
	            	
	                max = i - 1;
	                
	            }
	            page.updateminmax();
	            pageserialize(page);
	        }
	    
	        	return pagedeserialize(pagesbucket.get(index));	
	        
	       
	       
	    }
	
	 

	 
	 public String initfile () throws IOException{

			
				 String pagePath = "page_" + strTableName + pagespaths.size() + ".bin";
		            File file = new File(pagePath);
		            file.createNewFile();
		            pagespaths.add(pagePath);
		             return pagePath;
		            

			 
		 }
	 public void pageserialize(page page) {
//		    System.out.println("called");
		   
		    try {
		        FileOutputStream file = new FileOutputStream(page.getPagePath() );
		        ObjectOutputStream out = new ObjectOutputStream(file);
		        out.writeObject(page);
		        out.close();
		        file.close();
		    } catch (IOException e) {
//		    	System.out.println("page ser 8alat");
		        e.printStackTrace();
		     }

		}

		public page pagedeserialize(String pagepath) {
		  
		    try {
		        FileInputStream file = new FileInputStream(pagepath );
		        ObjectInputStream in = new ObjectInputStream(file);
		        page page = (page) in.readObject();
		        page.setPagePath(pagepath);
		        in.close();
		        file.close();
		        return page;
		    } catch (IOException | ClassNotFoundException e) {
		        e.printStackTrace();
//		        System.out.println("page deser 8alat");
		        return null;
		       
		    }
		}
		public void octreeserialize(Octree octree) {
//		    System.out.println("called");
		   
		    try {
		        FileOutputStream file = new FileOutputStream(octree.getOctreePath() +strTableName);
		        ObjectOutputStream out = new ObjectOutputStream(file);
		        out.writeObject(octree);
		        out.close();
		        file.close();
		    } catch (IOException e) {
		    	
		        e.printStackTrace();
		     }

		}

		public Octree octreedeserialize(String octreepath) {
		  
		    try {
		        FileInputStream file = new FileInputStream(octreepath + strTableName );
		        ObjectInputStream in = new ObjectInputStream(file);
		        Octree octree = (Octree) in.readObject();
		        octree.setOctreePath(octreepath );
		        in.close();
		        file.close();
		        return octree;
		    } catch (IOException | ClassNotFoundException e) {
		        e.printStackTrace();
		        
		        return null;
		       
		    }
		}
	 
	 
	 
	public static void main(String[] args) {
		Hashtable<String, Object> ht = new Hashtable<String, Object>();
		ht.put("name", "andrew");
		ht.put("age", 21);
		ht.put("car", "cn7");
//		System.out.println(ht);
//table t = new table("table1","name");
//		t.inserttable(ht);
		
		
	}

}

