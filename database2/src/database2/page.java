
package database2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Vector;

public class page implements Serializable {
	
  Vector <tuple> vec;
 public  String pageName;
 
 private String pagePath;

 public String getPagePath() {
	return pagePath;
}




public void setPagePath(String pagePath) {
	this.pagePath = pagePath;
}


static int maxPerPage;
 public Object min, max;
public int size;

 


 static { 
	 
	 
	 String fileName = "DBApp.config";
	 	String filePath = System.getProperty("user.dir") + "/src/" + "/database2/" + fileName;
//	 String filePath = "/Users/youannatarek/Desktop/database2/src/database2/DBApp.config";

 try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
     String line = br.readLine();
     if (line != null) {
         String[] parts = line.split("=");
         if (parts.length == 2) {
             String valueString = parts[1].trim();
             int value = Integer.parseInt(valueString);
//             System.out.println(value);
             maxPerPage=value;
         }
     }
 } catch (IOException e) {
     e.printStackTrace();
 }}
 

 
 public page(){
	 
	 vec=new Vector <tuple>(maxPerPage);
	   this.pageName = this.toString();
      
       min=null;
       max=null;

  
 
 }
 
 
 
 
 public boolean isfull() {
	 if (vec.size()==maxPerPage)
	 {
		 return true;
	 }
	 return false;
 }
 
 
 
 
 
 
// public void serialize() {
//     System.out.println("called");
//     if (vec == null)
//         return;
//     try {
//         FileOutputStream file = new FileOutputStream(pagePath);
//         ObjectOutputStream out = new ObjectOutputStream(file);
//         out.writeObject(vec);
//         out.close();
//         file.close();
//     } catch (IOException e) {
//         e.printStackTrace();
//      }
//
// }
//
// public void deserialize() {
//     if (vec != null) 
//         return;
//     try {
//         FileInputStream file = new FileInputStream(pagePath);
//         ObjectInputStream in = new ObjectInputStream(file);
//         vec = (Vector<tuple>) in.readObject();
//         in.close();
//         file.close();
//     } catch (IOException | ClassNotFoundException e) {
//         e.printStackTrace();
//        
//     }
// }


//public void free() {
//    // free the memory occupied be the page in the main memory
//    this.vec= null;
//}
//
//public void save() {
//   
//    this.serialize();
//}
//
//
//public void saveAndFree() {
//    // save to the disk then free from the main memory
//    this.save();
//    this.free();
//
//}
//public tuple mininpage(){
//	
//	tuple min=vec.get(0);
//	for (int i=0;i<vec.size();i++) {
//		if ((min.compareTo(vec.get(i)))<0)
//			min=vec.get(i);
//		
//	}
//	return min;
//		
//		
//	
//	
//	
//}
//public tuple maxinpage(){
//	
//	tuple max=vec.get(0);
//	for (int i=0;i<vec.size();i++) {
//		if ((max.compareTo(vec.get(i)))>0)
//			max=vec.get(i);
//	}
//	return max;
//	
//			
//}

  void updateminmax() {
    // update the size, min, max to avoid loading the pages every time
    this.size = vec.size();
    this.min = vec.firstElement().primkey;
    this.max = vec.lastElement().primkey;
}
//public static void main(String[] args) {
//	page p1=new page();
//	System.out.println(p1.maxPerPage);
//}

}