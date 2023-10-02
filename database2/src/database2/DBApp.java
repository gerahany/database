 package database2;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.JTable;



public class DBApp {

	static int maxPerPage;


	 
	   

	 
 public DBApp() throws IOException{
		
		
		}
	
// public static String initfile (String name) throws IOException{
//
//		
//	 String tablePath = name;
//	 System.out.println(tablePath);
//        File file = new File(tablePath);
//  
//        file.createNewFile();
//       
//         return tablePath;
//     }
	
	
 public void createTable(String strTableName,
			String strClusteringKeyColumn
			,Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax
			)throws DBAppException, IOException{
	
	 String fileName = "metadata.csv";
 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
    File file = new File(filePath);
		BufferedReader br=new BufferedReader(new FileReader(filePath));
		String line="";
		boolean check=true;
		boolean checktype=false;
		int sizemin=htblColNameType.size();
		int sizemax=htblColNameType.size();
		while((line=br.readLine())!=null) {
			String[]values=line.split(",");
			if(values[0].equals(strTableName)) {
				check=false;}}
		int lastIteration = htblColNameType.size() - 1;
		int i=0;
		Enumeration<String> keystype = htblColNameType.keys();
		while(keystype.hasMoreElements()) {
			String type=keystype.nextElement();
			if(htblColNameType.get(type).equals("java.lang.Integer") ||
					htblColNameType.get(type).equals("java.lang.String")||
					htblColNameType.get(type).equals("java.lang.Double")||
					htblColNameType.get(type).equals("java.util.Date")) {
				checktype=true;
				
			}
			if(i != lastIteration) {
				i++;
			if(checktype==true) checktype=false;
			else break;
			}
					
		}
		if(check && checktype) {
			
			 PrintWriter p=new PrintWriter(new FileWriter(file, true)); 
				Enumeration<String> mykeys = htblColNameType.keys();
				String min = null;
				String max=null;
				String prim=null;
				Enumeration<String> mykeyscheck = htblColNameType.keys();
				while(mykeyscheck.hasMoreElements()) {
					String keycheck=mykeyscheck.nextElement();
					
				
					Enumeration<String> mykeys2check = htblColNameMin.keys();
					while(mykeys2check.hasMoreElements()) {
						String key2check=mykeys2check.nextElement();
						if(key2check==keycheck) {
							sizemin--;
							
						}
					}
					Enumeration<String> mykeys3check = htblColNameMax.keys();
					while(mykeys3check.hasMoreElements()) {
						String key3check=mykeys3check.nextElement();
						if(key3check==keycheck) {
							sizemax--;
							
						}
					}
				}

					if(sizemin==0 && sizemax==0) {
				while(mykeys.hasMoreElements()) {
					String key=mykeys.nextElement();
					
				
					Enumeration<String> mykeys2 = htblColNameMin.keys();
					while(mykeys2.hasMoreElements()) {
						String key2=mykeys2.nextElement();
						if(key2==key) {
							 min= htblColNameMin.get(key2);
							
						}
					}
					Enumeration<String> mykeys3 = htblColNameMax.keys();
					while(mykeys3.hasMoreElements()) {
						String key3=mykeys3.nextElement();
						if(key3==key) {
							 max= htblColNameMax.get(key3);
							
						}
					}
					if(key==strClusteringKeyColumn) {
						 prim="True";
					}
					else {
						 prim="False";
					}
					
					 String newRow=  strTableName +"," +key+"," +htblColNameType.get(key)+"," +prim+"," +null+"," +null+"," + min +"," +max + "\n";	
			p.write(newRow);
					}
				}
				else { 
//					System.out.println("not same col");
				}
					
				p.close();
				
				table mytable = new table(strTableName,strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
				String tablepath=strTableName;
//				System.out.println(tablepath);
				mytable.setTablepath(tablepath);
				tableserialize(mytable);
		}
				else {
//					System.out.println("same table name");
					}
				
		
	 
		
	}
				
				
	
 
 
 public void init() throws IOException  {
	
	
	 
// String fileName = "metadata.csv";
//	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
//		 File file = new File(filePath);
//		 
//		 
//			
//	 PrintWriter p=new PrintWriter(new FileWriter(file, true)); 
//	
//	 BufferedReader br=new BufferedReader(new FileReader(filePath));
//	 String line;
//	 List<String[]> rows = new ArrayList<>();
//     while ((line = br.readLine()) != null) {
//         String[] row = line.split(",");
//         rows.add(row);
//     }
//	 
//     String[] firstRow = rows.get(0);
//	  
//	    if(firstRow.length == 0)
//	    p.write("TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType, min, max");
//	   p.close();
//         System.out.println("ana da5lt");
     
	   }
	
	
 
// if space
// mafrod nshuf low n3mlha binary search 
public void orderpage (tuple t,page p) {
		int i = 0;
		while (i < p.vec.size() && compareObjects(p.vec.get(i).getPrimkey() , t.getPrimkey())<0) {
		    i++;}

		 p.vec.add(null);
		    for (int j = p.vec.size() - 2; j >= i; j--) {
		        p.vec.set(j + 1, p.vec.get(j));
		    }
		    p.vec.set(i, t);
	    p.vec.set(i, t);;
	    p.updateminmax();
		  
		}
	
	
	

public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue)throws DBAppException, IOException, ParseException{
//	create tuple	
//	for (int i=0;i<)
	table mytable=tabledeserialize(strTableName);
	if(mytable!=null) {
	  if(checkcol(strTableName,htblColNameValue)==true) {
		 if(validate(strTableName,htblColNameValue)==true) {
			 Octree myoctree=null;
			 Object x=null;
				Object y=null;
				Object z=null;
			String fileName = "metadata.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
			 File file = new File(filePath);
				BufferedReader br=new BufferedReader(new FileReader(filePath));
		String line="";
		String primname=null;
		List<String> lines = new ArrayList<>();
		while((line=br.readLine())!=null) {
			String[]values=line.split(",");
			if(values[0].equals(strTableName) && values[3].equals("True")) {
				primname=values[1];}
			}
		line="";
		BufferedReader br2=new BufferedReader(new FileReader(filePath));
				while((line=br2.readLine())!=null) {
			String[]values=line.split(",");
		
			if(values[0].equals(strTableName)&& values[5].equals("Octree")) {
				

				if (!lines.contains(values[4])) {
			lines.add(values[4]);
		 myoctree=mytable.octreedeserialize(values[4]);
		Enumeration<String> mykeys = htblColNameValue.keys();
		
		
		tuple newtuple=null;

		while(mykeys.hasMoreElements()) {
			String key=mykeys.nextElement();
			if(key.equals(myoctree.col1)) {
				x=htblColNameValue.get(key);
			}
			if(key.equals(myoctree.col2)) {
				y=htblColNameValue.get(key);
			}
			if(key.equals(myoctree.col3)) {
				z=htblColNameValue.get(key);
			}
			if(key.equals(primname)){
//				System.out.println("ana ba3ml new tuple");
			 newtuple= new tuple( htblColNameValue.get(key),htblColNameValue);
//			 System.out.println(newtuple.getPrimkey());
			 }
			}
		if(!mytable.pagesbucket.isEmpty()) {
		page supposedpage=mytable.searchforPage(newtuple);
		if(mytable.pagesbucket.indexOf(supposedpage.getPagePath())!=mytable.pagesbucket.size()-1) {
//			System.out.println("msh akhr page ");
		int pageindex=mytable.pagesbucket.indexOf(supposedpage.getPagePath());
		if(compareObjects(supposedpage.vec.get(supposedpage.vec.size()-1).primkey,newtuple.primkey)<0) {
			supposedpage.updateminmax();
			mytable.pageserialize(supposedpage);
			supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(pageindex+1));
			pageindex=pageindex+1;
		}}
		
			myoctree.insert(x, y, z, supposedpage.getPagePath());
			myoctree.printAllNodes();
			mytable.octreeserialize(myoctree);
			mytable.octreeserialize(myoctree);
			mytable.pageserialize(supposedpage);		
		}
				}}

			if(values[0].equals(strTableName) && values[3].equals("True")) {
				primname=values[1];}
			}
		Enumeration<String> mykeys = htblColNameValue.keys();
		tuple newtuple=null;
		
		while(mykeys.hasMoreElements()) {
			String key=mykeys.nextElement();
			if(key.equals(primname)){
//				System.out.println("ana ba3ml new tuple");
			 newtuple= new tuple( htblColNameValue.get(key),htblColNameValue);
//			 System.out.println(newtuple.getPrimkey());
			 }
            }
//		System.out.println(newtuple);
//	1st page		
		if (mytable.pagesbucket.isEmpty()) {
			page newpage=new page();
			newpage.setPagePath(mytable.initfile());
			newpage.vec.add(newtuple);
//			System.out.println(newtuple.rowdata);
			mytable.pagesbucket.add(newpage.getPagePath());
			newpage.updateminmax();
			if(myoctree !=null) {
			myoctree.insert(x, y, z, newpage.getPagePath());
			myoctree.printAllNodes();
			mytable.octreeserialize(myoctree);
			mytable.octreeserialize(myoctree);}
			mytable.pageserialize(newpage);
//			System.out.println("add page");
			}
// b2at el pages
		else {
//			System.out.println("b2at el pages page");
			page supposedpage=mytable.searchforPage(newtuple);
// low feh mkan fl pages
			if (!supposedpage.isfull()) {
			orderpage(newtuple,supposedpage);
			supposedpage.updateminmax();
			mytable.pageserialize(supposedpage);
//			System.out.println("feh mkan fl page");
			}
			else {
//				System.out.println("mafesh mkan fl page");
				
				page lastpage=mytable.pagedeserialize(mytable.pagesbucket.get(mytable.pagesbucket.size()-1));
				tuple oldtemptuple=null;
				tuple newtemptuple=null;
				if(mytable.pagesbucket.indexOf(supposedpage.getPagePath())!=mytable.pagesbucket.size()-1) {
//					System.out.println("msh akhr page ");
					int pageindex=mytable.pagesbucket.indexOf(supposedpage.getPagePath());
					if(compareObjects(supposedpage.vec.get(supposedpage.vec.size()-1).primkey,newtuple.primkey)<0) {
						supposedpage.updateminmax();
						mytable.pageserialize(supposedpage);
						supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(pageindex+1));
						pageindex=pageindex+1;
					}
					if(mytable.pagesbucket.indexOf(supposedpage.getPagePath())==mytable.pagesbucket.size()-1) {
//						System.out.println("low waslna lakhr page");
						if(!lastpage.isfull()) {
//							System.out.println("akhr page msh malyana");
							orderpage(newtuple,lastpage);
							supposedpage.updateminmax();
							mytable.pageserialize(lastpage);
							
						}
						else {
							
							
							
//							System.out.println("akhr page malyna");
							
							page newpage=new page();
							newpage.setPagePath(mytable.initfile());
							mytable.pagesbucket.add(newpage.getPagePath());
							tuple laste=lastpage.vec.get(lastpage.vec.size()-1);
							newpage.vec.add(laste);
							newpage.updateminmax();
							mytable.pageserialize(newpage);
							lastpage.vec.remove(lastpage.vec.size()-1);
							orderpage(newtuple,lastpage);
							lastpage.updateminmax();
							mytable.pageserialize(lastpage);
						}
						
					}
					else {
					
//				System.out.println("low mawslnash lakhr page");
				oldtemptuple=newtuple;
				
				
				for(int i=pageindex;i<mytable.pagesbucket.size()-1;i++) {
					newtemptuple=supposedpage.vec.get((supposedpage.vec.size())-1);
					supposedpage.vec.remove((supposedpage.vec.size())-1);
					orderpage(oldtemptuple,supposedpage);
					mytable.pageserialize(supposedpage);
					supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(i+1));
					oldtemptuple=newtemptuple;
				}
				
				
				
				if(!lastpage.isfull()) {
//					System.out.println("akhr page feha mkan");
					orderpage(oldtemptuple,lastpage);
					mytable.pageserialize(lastpage);
				}
				else {
//					System.out.println("akhr page mafhash mkan");
					page newpage=new page();
					newpage.setPagePath(mytable.initfile());
					mytable.pagesbucket.add(newpage.getPagePath());
	
					tuple laste=lastpage.vec.get(lastpage.vec.size()-1);
					newpage.vec.add(laste);
					newpage.updateminmax();
					mytable.pageserialize(newpage);
					lastpage.vec.remove(lastpage.vec.size()-1);
					orderpage(oldtemptuple,lastpage);
					lastpage.updateminmax();
					mytable.pageserialize(lastpage);
					
				}
				}
					}
				else {
//					System.out.println("akhr page");
					if(!supposedpage.isfull()) {
//						System.out.println("akhr page feha mkan");
						orderpage(newtuple,supposedpage);
						mytable.pageserialize(supposedpage);
					}
					else {
//						System.out.println("a5er page mafhash mkan");
						page newpage=new page();
						newpage.setPagePath(mytable.initfile());
						mytable.pagesbucket.add(newpage.getPagePath());
						tuple laste=supposedpage.vec.get(supposedpage.vec.size()-1);
						if(compareObjects(supposedpage.vec.get(supposedpage.vec.size()-1).primkey,newtuple.primkey)<0) {
//							System.out.println("low el rakhm akbr mn akhr rakm");
							newpage.vec.add(newtuple);
							newpage.updateminmax();
							mytable.pageserialize(newpage);}
						else {
//							System.out.println("low el rakm msh akbr mn akhr rakm");
							supposedpage.vec.remove(supposedpage.vec.size()-1);
					
						orderpage(newtuple,supposedpage);
						mytable.pageserialize(supposedpage);
						newpage.vec.add(laste);
						newpage.updateminmax();
						mytable.pageserialize(newpage);
						}
						
					}
				}
				
				
				
				}
		}

         
//		System.out.println(mytable.pagesbucket);		

	}
		 else {
//			 System.out.println("unvalid data");
		 }
		 }
//		else System.out.println("unvalid col");
	  tableserialize(mytable); } }



public static int compareObjects(Object obj1, Object obj2) {
	  if (obj1 == null || obj2 == null) {
	    // handle null objects
	    return 0;	  }

	  if (obj1 instanceof Integer && obj2 instanceof Integer) {
	    // compare integers
	    return ((Integer) obj1).compareTo((Integer) obj2);
	  }
	  if (obj1 instanceof String && obj2 instanceof String) {
	    // compare strings
	    return ((String) obj1).compareTo((String) obj2);
	  }
	  if (obj1 instanceof Double && obj2 instanceof Double) {
	    // compare doubles
	    return ((Double) obj1).compareTo((Double) obj2) ;
	  }
	  if (obj1 instanceof Date && obj2 instanceof Date) {
	    // compare dates
	    return ((Date) obj1).compareTo((Date) obj2);
	  }
	  // handle unsupported types
	  throw new IllegalArgumentException("Unsupported types: " + obj1.getClass().getName() + ", " + obj2.getClass().getName());
	}





public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue)throws DBAppException, IOException{
	String line="";
String primname=null;
String fileName = "metadata.csv";
String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
 File file = new File(filePath);
	BufferedReader brr=new BufferedReader(new FileReader(filePath));
	boolean check=false;
	boolean checkprim=true;
	boolean checkindex=false;
	 int countindex=0;
	while((line=brr.readLine())!=null) {
		String[]values=line.split(",");

			
		
		if(values[0].equals(strTableName) ) {
			check=true;
			if(values[3].equals("True")) {
				primname=values[1];
			}
		}
		
		
	}
	
	
	 if (check==false) {
		 
	 }
//		 System.out.println("no existing table");
		else {
			Enumeration<String> mykeysprim = htblColNameValue.keys();
			
			
			while(mykeysprim.hasMoreElements()) {
				String keyprim=mykeysprim.nextElement();
				if(keyprim.equals(primname)){
			checkprim=false;
				}}
			
			table mytable=tabledeserialize(strTableName);
			if(mytable !=null) {
				 boolean ifequal1=true;
				 Vector <String> octreenames =new Vector<String>();
				 for(int i=0;i<mytable.indexpath.size();i++) {
					  countindex=0;
				 Enumeration<String> mykeys2 = htblColNameValue.keys();
				while(mykeys2.hasMoreElements()) {
					String key=mykeys2.nextElement();
					BufferedReader brr2=new BufferedReader(new FileReader(filePath));
					line="";
					while((line=brr2.readLine())!=null) {
						String[]values=line.split(",");
						if(values[1].equals(key) && values[0].equals(strTableName) ) {
							
							if(values[5].equals("Octree") && values[4].equals(mytable.indexpath.get(i)) ) {
							countindex++;
							
							}
							}
						}
				}
				if(!octreenames.contains(mytable.indexpath.get(i))) {
					octreenames.add(mytable.indexpath.get(i));}
				if(countindex==3) {
					
					checkindex=true;
				}
				}

				if(checkindex==true) {
//					System.out.println("index");
					
					int found1=0;
					
					
						
//						Enumeration<String> mykeyshash = htblColNameValue.keys();
//						while(mykeyshash.hasMoreElements()) {
//							String key=mykeyshash.nextElement();
//							BufferedReader brr3=new BufferedReader(new FileReader(filePath));
//							line="";
							
//								if(values[1].equals(key) && values[5].equals("Octree")) {
//									String name=values[4];
									for(int n=0;n<octreenames.size();n++) {
										
									
										Octree myoctree=mytable.octreedeserialize(octreenames.get(n));
										String x2=myoctree.col1;
										String y2=myoctree.col2;
										String z2=myoctree.col3;
										Object xvalue2=null;
										Object yvalue2=null;
										Object zvalue2=null;
										
										//ta8yr el type:
										Enumeration<String> mykeyshash = htblColNameValue.keys();
										while(mykeyshash.hasMoreElements()) {
											String key=mykeyshash.nextElement();
											BufferedReader brr3=new BufferedReader(new FileReader(filePath));
											line="";
											BufferedReader brr2=new BufferedReader(new FileReader(filePath));
											line="";
											while((line=brr2.readLine())!=null) {
												String[]values=line.split(",");
												if(values[0].equals(strTableName) && key.equals(values[1])){
													String mytype=values[2];
													Object o1=convertStringToType((( htblColNameValue.get(key)).toString()), mytype);
													if(key.equals(x2)) {
														
														 xvalue2=o1;}
													if(key.equals(y2)) {
														 yvalue2=o1;}
													if(key.equals(z2)) {
														 zvalue2=o1;}
													
													
												}
											}
										}
										
										
										List<String> mylist=myoctree.searchElements(myoctree.root,xvalue2, yvalue2, zvalue2);
										for(int i=0;i<mylist.size();i++) {
											page mypage=mytable.pagedeserialize(mylist.get(i));
											for(int j=0;j<mypage.vec.size();j++) {
												Hashtable<String,Object> myhash=mypage.vec.get(j).getRowdata();
												 found1=0;
												
										
												
													Enumeration<String> mykeyshash2 = htblColNameValue.keys();
													
													while(mykeyshash2.hasMoreElements()) {
														String keyhash=mykeyshash2.nextElement();
														Enumeration<String> mykeysmyhash = myhash.keys();
														while(mykeysmyhash.hasMoreElements()) {
															String keymyhash=mykeysmyhash.nextElement();
															if(keyhash.equals(keymyhash)) {
																String mytype2= myhash.get(keymyhash).getClass().getName();
																Object o12=convertStringToType((( htblColNameValue.get(keyhash)).toString()), mytype2);
																Object o13=convertStringToType((( myhash.get(keymyhash)).toString()), mytype2);
																if(compareObjects(o13,o12)==0) {
																	
																	found1++;
																}
																	
																}
															}
														}
													
												if( found1==htblColNameValue.size()) {
													mypage.vec.remove(j);
													String x=myoctree.col1;
													String y=myoctree.col2;
													String z=myoctree.col3;
													Enumeration<String> mykeysmyhash = myhash.keys();
													Object xvalue=null;
													Object yvalue=null;
													Object zvalue=null;
													while(mykeysmyhash.hasMoreElements()) {
														String keymyhash=mykeysmyhash.nextElement();
														if(keymyhash.equals(x)) {
															 xvalue=myhash.get(keymyhash);}
														if(keymyhash.equals(y)) {
															 yvalue=myhash.get(keymyhash);}
														if(keymyhash.equals(z)) {
															 zvalue=myhash.get(keymyhash);}
														}
													myoctree.remove(xvalue, yvalue, zvalue);
													
													
													j--;}	}
											if(mypage.vec.size()==0) {
												mytable.pagesbucket.remove(i);
												i--;
											}
											else {
												mypage.updateminmax();
												mytable.pageserialize(mypage);	}
												
											}
											
										
										myoctree.printAllNodes();
									mytable.octreeserialize(myoctree);}
									
								
						}
					
					

			
			else {
//		System.out.println("no index");
//		for(int f=0;f<octreenames.size();f++) {
//			Octree myoctree=mytable.octreedeserialize(octreenames.get(f));
//			  int cc=0;
//				 Enumeration<String> mykeys2 = htblColNameValue.keys();
//				while(mykeys2.hasMoreElements()) {
//					String key=mykeys2.nextElement();
//					BufferedReader brr2=new BufferedReader(new FileReader(filePath));
//					line="";
//					while((line=brr2.readLine())!=null) {
//						String[]values=line.split(",");
//						if(values[1].equals(key) && values[0].equals(strTableName) ) {
//							
//							if(values[5].equals("Octree") && values[4].equals(octreenames.get(f)) ) {
//								cc++;
//							
//							}
//							}
//						}
//				}
//				if(cc==1 || cc==2) {
//					Hashtable <String ,Object> getxyz= new Hashtable <String ,Object>();
//					for(int i=0;i<mytable.pagesbucket.size();i++) {
//						page supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
//						for (int s=0;s<supposedpage.vec.size();s++) {
//							String mytype= supposedpage.vec.get(s).primkey.getClass().getName();
//							Object o1=convertStringToType(.toString()), mytype);
//							if(compareObjects(myhashtable.get(keymyhash),o1)==0) {
//								
//								found++;
//							}
//						}
//						mytable.pageserialize(supposedpage);
//					}
//					
//				}
//			myoctree.remove(xvalue, yvalue, zvalue);
//			
//		}
		
		if (!mytable.pagesbucket.isEmpty()) {
			
			
			for(int i=0;i<mytable.pagesbucket.size();i++) {
				 page supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
				 boolean ifequal=true;
for(int j=0;j<supposedpage.vec.size();j++) {
	int found=0;
				Hashtable myhashtable=supposedpage.vec.get(j).getRowdata();
				Enumeration<String> mykeyshash = htblColNameValue.keys();
				
				while(mykeyshash.hasMoreElements()) {
					String keyhash=mykeyshash.nextElement();
					Enumeration<String> mykeysmyhash = myhashtable.keys();
					while(mykeysmyhash.hasMoreElements()) {
						String keymyhash=mykeysmyhash.nextElement();
						if(keyhash.equals(keymyhash)) {
							String mytype= myhashtable.get(keymyhash).getClass().getName();
							Object o1=convertStringToType((( htblColNameValue.get(keyhash)).toString()), mytype);
							Object o2=convertStringToType(((myhashtable.get(keymyhash).toString())), mytype);
							if(compareObjects(o2,o1)==0) {
								
								found++;
							}
								
							}
						}
					}
				if(ifequal==true && found==htblColNameValue.size()) {
					for(int f=0;f<octreenames.size();f++) {
						Octree myoctree=mytable.octreedeserialize(octreenames.get(f));
						  int cc=0;
							 Enumeration<String> mykeys2 = htblColNameValue.keys();
							while(mykeys2.hasMoreElements()) {
								String key=mykeys2.nextElement();
								BufferedReader brr2=new BufferedReader(new FileReader(filePath));
								line="";
								while((line=brr2.readLine())!=null) {
									String[]values=line.split(",");
									if(values[1].equals(key) && values[0].equals(strTableName) ) {
										
										if(values[5].equals("Octree") && values[4].equals(octreenames.get(f)) ) {
											cc++;
										
										}
										}
									}
							}
							String x=myoctree.col1;
							String y=myoctree.col2;
							String z=myoctree.col3;
							Object xvalue=null;
							Object yvalue=null;
							Object zvalue=null;
							if(cc==1 || cc==2) {
								Enumeration<String> skey =  supposedpage.vec.get(j).getRowdata().keys();
								while(skey.hasMoreElements()) {
									String key=skey.nextElement();
									if(key.equals(x)) {
										xvalue=supposedpage.vec.get(j).getRowdata().get(key);}
									if(key.equals(y)) {
										yvalue=supposedpage.vec.get(j).getRowdata().get(key);}
									if(key.equals(z)) {
										zvalue=supposedpage.vec.get(j).getRowdata().get(key);}
									}
								myoctree.remove(xvalue, yvalue, zvalue);
								
							}
							
							mytable.octreeserialize(myoctree);}
				supposedpage.vec.remove(j);
				j--;}
				
				}
if(supposedpage.vec.size()==0) {
	mytable.pagesbucket.remove(i);
	i--;
}
else {
	supposedpage.updateminmax();
	mytable.pageserialize(supposedpage);	}	
			}
		}
		
		
			}
}tableserialize(mytable);
		
			}
}


public static Object convertStringToType(String s1, String s2) throws DBAppException {
    Object result = null;
    try {
        switch (s2) {
            case "java.lang.Integer":
                result = Integer.parseInt(s1);
                break;
            case "java.lang.Double":
                result = Double.parseDouble(s1);
                break;
            case "java.util.Date":
//            	System.out.println("fhm eno date");
//                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//                result = dateFormat.parse(s1);
            	
            	 SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                 try {
                     result = dateFormat.parse(s1);
                 } catch (ParseException e) {
                     // Try to parse date string in "EEE MMM dd HH:mm:ss zzz yyyy" format
                     SimpleDateFormat dateFormat2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
                     Date date = dateFormat2.parse(s1);
                     // Format date to "yyyy-MM-dd" format
                     result = dateFormat.format(date);
                 }
                break;
            case "java.lang.String":
                result = s1;
                break;
            default:
                System.err.println("Invalid type: " + s2);
        }
    } catch (Exception e) {
        System.err.println("Failed to convert '" + s1 + "' to type '" + s2 + "': " + e.getMessage());
        throw  new DBAppException();
    }
    return result;
}

//
//public static Object convertStringToType(String s1, String s2) {
//    Object result = null;
//    try {
//        switch (s2) {
//            case "java.lang.Integer":
//                result = Integer.parseInt(s1);
//                break;
//            case "java.lang.Double":
//                result = Double.parseDouble(s1);
//                break;
//            case "java.util.Date":
//                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//                result = dateFormat.parse(s1);
//                break;
//            case "java.lang.String":
//                result = s1;
//                break;
//            default:
//                System.err.println("Invalid type: " + s2);
//        }
//    } catch (Exception e) {
//        System.err.println("Failed to convert '" + s1 + "' to type '" + s2 + "': " + e.getMessage());
//    }
//    return result;
//}







public void updateTable(String strTableName,
String strClusteringKeyValue,
Hashtable<String,Object> htblColNameValue )
throws DBAppException, IOException, ParseException{
	
	String linee="";
	String primname=null;
	String fileNamee = "metadata.csv";
	String filePathe = System.getProperty("user.dir") + "/resources/" + fileNamee;
	 File filee = new File(filePathe);
		BufferedReader brr=new BufferedReader(new FileReader(filePathe));
		boolean check=false;
		boolean checkprim=true;
		
		while((linee=brr.readLine())!=null) {
			String[]values=linee.split(",");

				
			
			if(values[0].equals(strTableName) ) {
				check=true;
				if(values[3].equals("True")) {
					primname=values[1];
				}
			}
			
			
		}
		
	Enumeration<String> mykeysprim = htblColNameValue.keys();
	
	
	while(mykeysprim.hasMoreElements()) {
		String keyprim=mykeysprim.nextElement();
		if(keyprim.equals(primname)){
	checkprim=false;
		}}
	table mytable=tabledeserialize(strTableName);
	if(mytable!=null) {
	if(checkprim) {
	
	 if(validate(strTableName,htblColNameValue)==true){
//		System.out.println("da5lna fl update");
		
		page supposedpage=null;
		String mytype=null;
		Object primkey=null;
	
		
		String fileName = "metadata.csv";
 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
			 File file = new File(filePath);
				BufferedReader br=new BufferedReader(new FileReader(filePath));
			String line="";
			
			while((line=br.readLine())!=null) {
				String[]values=line.split(",");
				if(values[0].equals(strTableName) && values[3].equals("True")) {
					mytype=values[2];}
				}
			
			primkey=convertStringToType(strClusteringKeyValue,mytype);
			tuple newtuple=new tuple(primkey,htblColNameValue);
			
			supposedpage=mytable.searchforPage(newtuple);
			int pageindex=mytable.pagesbucket.indexOf(supposedpage.getPagePath());
			page lastpage=mytable.pagedeserialize(mytable.pagesbucket.get(mytable.pagesbucket.size()-1));
			int lastindex=mytable.pagesbucket.indexOf(lastpage.getPagePath());
			if(compareObjects(supposedpage.vec.get(supposedpage.vec.size()-1).primkey,newtuple.primkey)<0 && pageindex!=lastindex  ) {
				supposedpage.updateminmax();
				mytable.pageserialize(supposedpage);
				supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(pageindex+1));
				pageindex=pageindex+1;
			}
			lastpage.updateminmax();
			mytable.pageserialize(lastpage);
				
			for (int i=0;i<supposedpage.vec.size();i++) {
				if(compareObjects(supposedpage.vec.get(i).getPrimkey(),newtuple.getPrimkey())==0) {
tuple mytuple =supposedpage.vec.get(i);
//					System.out.println("3andaha nafs el primarykey");

				
				Hashtable <String,Object> makehash=new Hashtable <String,Object>();
				Enumeration<String> mykeys2 = supposedpage.vec.get(i).getRowdata().keys();
						
						
				while(mykeys2.hasMoreElements()) {
					String key2=mykeys2.nextElement();
					boolean found=false;
				Enumeration<String> mykeys = htblColNameValue.keys();
				
				while(mykeys.hasMoreElements()) {
					String key=mykeys.nextElement();
					if(key2.equals(key)) {
						found=true;
					}

					}
				if(found==false) {
					
					newtuple.getRowdata().put(key2, supposedpage.vec.get(i).getRowdata().get(key2));}
				}
//				System.out.println(newtuple.getRowdata());
//				System.out.println(mytuple.getRowdata());
				mytable.pageserialize(supposedpage);
				tableserialize(mytable);
				
				deleteFromTable(strTableName,mytuple.getRowdata());
				insertIntoTable(strTableName, newtuple.getRowdata());
				
//				if(supposedpage.vec.get(i).getRowdata().containsKey(key)) {
//					supposedpage.vec.get(i).getRowdata().replace(key, value);	
//					System.out.println(supposedpage.vec.get(i).getRowdata());
//
				mytable=tabledeserialize(strTableName);
				
					
					tableserialize(mytable);
					
//					}
					
//					}
					
				}
				else {
//					System.out.println("primkey not in table");
				}
				
			}
			
		
		
		
	
	 }
	else {
//		System.out.println("unvalid data");
	}
	}
	else {
		
	}
//		System.out.println("cant update pk");
	
	}}
public boolean checkcol(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException {
	String fileName = "metadata.csv";
 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
	 File file = new File(filePath);
		BufferedReader br=new BufferedReader(new FileReader(filePath));
	String line="";
	int colsize=0;
	while((line=br.readLine())!=null) {
		String[]values=line.split(",");
		if (values[0].equals(strTableName)) {
			colsize++;
		}
	}
	Enumeration<String> mykeys = htblColNameValue.keys();
while(mykeys.hasMoreElements()) {
		
		String key=mykeys.nextElement();
	     line="";
		
		BufferedReader brr=new BufferedReader(new FileReader(filePath));
		
	while((line=brr.readLine())!=null) {
		String[]values=line.split(",");
	if(values[0].equals(strTableName) && values[1].equals(key)) {
		colsize--;
			
		}
	}}
if(colsize==0) return true;
else {
//	System.out.println("not same num of col");
	return false;
}
	
}


	
public static boolean validate(String strTableName,Hashtable<String,Object> htblColNameValue) throws IOException, ParseException, DBAppException	{
	boolean result=false;
	String primname=null;
	Object maxvale= null;
	Object minvalue=null;
	Enumeration<String> mykeys = htblColNameValue.keys();
	tuple newtuple=null;
	int i = 0;
	int lastIteration = htblColNameValue.size() - 1;
	table mytable=tabledeserialize(strTableName);
	while(mykeys.hasMoreElements()) {
		
		String key=mykeys.nextElement();
		String line="";
		String fileName = "metadata.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
		 File file = new File(filePath);
			BufferedReader br=new BufferedReader(new FileReader(filePath));
	while((line=br.readLine())!=null) {
		String[]values=line.split(",");
		

		//check type
		
			
		 
		if(values[0].equals(strTableName) && values[1].equals(key)) {
			
			if(mytable.pagesbucket.size()>0) {
			if(values[3].equals("True")) {
				if(key.equals(values[1])){
					newtuple= new tuple( htblColNameValue.get(key),htblColNameValue);
					page supposedpage=mytable.pagedeserialize(mytable.searchforPage(newtuple).getPagePath());
					int pageindex=mytable.pagesbucket.indexOf(supposedpage.getPagePath());
					page lastpage=mytable.pagedeserialize(mytable.pagesbucket.get(mytable.pagesbucket.size()-1));
					int lastindex=mytable.pagesbucket.indexOf(lastpage.getPagePath());
					if(compareObjects(supposedpage.vec.get(supposedpage.vec.size()-1).primkey,newtuple.primkey)<0 && pageindex!=lastindex  ) {
						supposedpage.updateminmax();
						mytable.pageserialize(supposedpage);
						supposedpage=mytable.pagedeserialize(mytable.pagesbucket.get(pageindex+1));
						pageindex=pageindex+1;
					}
					lastpage.updateminmax();
					mytable.pageserialize(lastpage);
					for(int k=0;k<supposedpage.size;k++) {
						if(compareObjects(supposedpage.vec.get(k).getPrimkey(),htblColNameValue.get(key))==0){
							return false;
						}
					}
				}
			}
			
			}
			if(htblColNameValue.get(key).getClass().getName().equals(values[2])){
//				System.out.println("true1");
				result =true;
			}
			else {
				if(htblColNameValue.get(key).getClass().getName().equals("java.lang.String")) {
					Object o1=convertStringToType(htblColNameValue.get(key).toString(),values[2].toString());
					if(o1==null) {
						return false;
						
					}
					else {
						result= true;
//						System.out.println("true2");
						htblColNameValue.replace(key,o1);
//						System.out.println((htblColNameValue.get(key).getClass().getName()));
					}
					
					
							
				}
				else {
					return false;
				}
					
			}
			
			if(result==true) {
//				System.out.println("hathsak low el average mazbot" );
				if (htblColNameValue.get(key).getClass().getName().equals("java.lang.String")){
				
					
				if (htblColNameValue.get(key).toString().compareTo(values[6])>=0 && htblColNameValue.get(key).toString().compareTo(values[7])<=0 ){
//					System.out.println("in range");
					result=true;
				}
					
				
				else {
//					System.out.println("out of range");
					return false;
					
				}
				}
				else if(htblColNameValue.get(key).getClass().getName().equals("java.lang.Integer") ){
					double min=Double.parseDouble(values[6]);
					double max=Double.parseDouble(values[7]);
					int myvalue=(int) htblColNameValue.get(key);
					if(myvalue>=min &&myvalue<=max) {
						result=true;
//						System.out.println("in range");
					}
					else {
//						System.out.println("out of range");
						return false;
					}
						
					
				}
				else if( htblColNameValue.get(key).getClass().getName().equals("java.lang.Double")){
					double min=Double.parseDouble(values[6]);
					double max=Double.parseDouble(values[7]);
					double myvalue=(double) htblColNameValue.get(key);
					if(myvalue>=min &&myvalue<=max) {
						result=true;
//						System.out.println("in range");
					}
					else {
//						System.out.println("out of range");
						return false;
					}
						
					
				}
				else if (htblColNameValue.get(key).getClass().getName().equals("java.util.Date")) {
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					Date mindate=format.parse(values[6]);
					Date maxdate=format.parse(values[7]);
					Date myvalue=(Date) htblColNameValue.get(key);
					if (myvalue.compareTo(mindate)>=0 && myvalue.compareTo(maxdate)<=0 ){
//						System.out.println("in range");
						result=true;
					}
						
					
					else {
//						System.out.println("out of range");
						return false;
					}
				}
				
			
			}
				
			}
		
		
		
		if (result==true)break;
		}
	if(i != lastIteration) {
		i++;
	if(result==true) result=false;
	else break;
	}
	}
//	tableserialize(mytable);
	return result;
	
	
	
	

}



public static void tableserialize(table table) {
//    System.out.println("called");
    
    try {
        FileOutputStream file = new FileOutputStream(table.getTablepath() );
//        System.out.println(table.getTablepath());
        ObjectOutputStream out = new ObjectOutputStream(file);
        out.writeObject(table);
        out.close();
        file.close();
    } catch (IOException e) {
        e.printStackTrace();
     }

}

public static table tabledeserialize(String tablepath) {
  
    try {
        FileInputStream file = new FileInputStream(tablepath );
//        System.out.println(tablepath);
        ObjectInputStream in = new ObjectInputStream(file);
        table table= (table) in.readObject();
        table.setTablepath(tablepath);
        in.close();
        file.close();
        return table;
    } catch (Exception e) {
//    	System.out.println("no table");
//        e.printStackTrace();
        return null;
       
    }
}


public static void printTableContents(String tableName) throws IOException {
	 table table= tabledeserialize(tableName); //deserializes table
	 
	 for(int i=0; i<table.pagesbucket.size(); i++) {
		 page p=table.pagedeserialize(table.pagesbucket.get(i));
		 System.out.printf("\n \nPage "+i+ " contents are: \n");
		 for(int j=0; j<p.vec.size();j++) {
		 System.out.println(p.vec.get(j).rowdata);
			 
		 }
	 }
	 tableserialize(table);
}

public void createIndex(String strTableName,
String[] strarrColName) throws DBAppException, IOException{
	if (strarrColName.length != 3) {
        throw new DBAppException();
    }
	else {
		table mytable= tabledeserialize(strTableName);
		if(mytable!=null) {
			String fileName = "metadata.csv";
		 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
			 File file = new File(filePath);
				
			String line="";
			int colsize=3;
			
				for(int i=0;i<strarrColName.length;i++) {
					BufferedReader br=new BufferedReader(new FileReader(filePath));
				line="";	
			while((line=br.readLine())!=null) {
				String[]values=line.split(",");
			if(values[0].equals(strTableName) && values[1].equals(strarrColName[i])) {
				colsize--;
				break;
					
				}
			}}
		if(colsize==0) {
			

			
				Object minvaluex = null;
				Object maxvaluex=null;
				Object minvaluey=null;
				Object maxvaluey=null;
				Object minvaluez=null;
				Object maxvaluez=null;
				
				
				
					
						
						
						
							
								
				 line="";
				
					BufferedReader brr=new BufferedReader(new FileReader(filePath));
					
					
					while((line=brr.readLine())!=null) {
						String[]values2=line.split(",");
									if(values2[1].equals(strarrColName[0])) {
										
											minvaluex=values2[6];
											maxvaluex=values2[7];
											
										}
									else if(values2[1].equals(strarrColName[1])) {
											minvaluey=values2[6];
											maxvaluey=values2[7];
											
										}
									else if(values2[1].equals(strarrColName[2])) {
											minvaluez=values2[6];
											maxvaluez=values2[7];
											
										}
									}
									
									
								
							
	
//		    saveIndex(octreeIndex, strTableName, strarrColName[0] + ""+ strarrColName[1] + "" + strarrColName[2] + "" + "index");
		   Octree myoctree=new Octree(minvaluex,maxvaluex,minvaluey,maxvaluey,minvaluez,maxvaluez);
		myoctree.col1=strarrColName[0];
		myoctree.col2=strarrColName[1];
		myoctree.col3=strarrColName[2];
		    

		    BufferedReader br3 = new BufferedReader(new FileReader(filePath));
		    List<String> lines = new ArrayList<>();
		    String line3 = "";
		    String primname = null;
		    String octreename=null;
		    while ((line3 = br3.readLine()) != null) {
		        String[] values = line3.split(",");
		        if (values[0].equals(strTableName) && (values[1].equals(strarrColName[0]) || values[1].equals(strarrColName[1]) || values[1].equals(strarrColName[2]))) {
		           octreename=strarrColName[0] + "" + strarrColName[1] + "" + strarrColName[2] + "" + "index";
		        	values[4] = octreename;
		            values[5] = "Octree";
		        }
		        lines.add(String.join(",", values));
		    }
		    br3.close();
		    BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
		    for (String linez : lines) {
		        bw.write(linez);
		        bw.newLine();
		    }
		    bw.close(); 
		    mytable.indexpath.add(octreename);

		    if(!mytable.pagesbucket.isEmpty()) {
		    	for(int t=0;t<mytable.pagesbucket.size();t++){
		    		page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(t));
		    		for(int h=0;h<mypage.vec.size();h++) {
		    			Hashtable<String,Object> myhashtable=mypage.vec.get(h).getRowdata();
		    			Enumeration<String> mykeys = myhashtable.keys();
		    			Object x=null;
		    			Object y=null;
		    			Object z=null;
		    			while(mykeys.hasMoreElements()) {
		    				String key=mykeys.nextElement();
		    				if(key.equals(myoctree.col1)) {
		    					x=myhashtable.get(key);
		    				}
		    				if(key.equals(myoctree.col2)) {
		    					y=myhashtable.get(key);
		    				}
		    				if(key.equals(myoctree.col3)) {
		    					z=myhashtable.get(key);
		    				}
		    				
		    				}
		    			myoctree.insert(x, y, z, mypage.getPagePath());
		    		}
		    	}
		    }
		    myoctree.setOctreePath(octreename);
		    myoctree.printAllNodes();
		    mytable.octreeserialize(myoctree);
		    }
		else {
//			System.out.println("not same  col");
			
		}
			
		tableserialize(mytable);}
		
		
		
	}
}

public boolean whatop (String op,Object v1,Object v2) {
	switch(op) {
	case "=" : if(compareObjects(v1,v2)==0) return true;
	break;
	case "!=" : if(compareObjects(v1,v2)!=0) return true;
	break;
	case ">=" : if(compareObjects(v1,v2)>=0) return true;
	break;
	case "<=" : if(compareObjects(v1,v2)<=0) return true;
	break;
	case ">" : if(compareObjects(v1,v2)>0) return true;
	break;
	case "<" : if(compareObjects(v1,v2)<0) return true;
	break;
	
	
}
	return false;}


public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
String[] strarrOperators)
throws DBAppException, IOException{
	if(arrSQLTerms.length==strarrOperators.length+1) {
		boolean ifalland=true;
		for(int i=0;i<strarrOperators.length;i++) {
			if(!strarrOperators[i].equals("AND")) {
				ifalland=false;
				break;
			}
		}
		if(strarrOperators.length==0) {
			ifalland=false;
		}
			
		if(ifalland==false) {
			ArrayList<tuple> res = new ArrayList<>();
			ArrayList<tuple> tempres = new ArrayList<>();
			for (int s=0;s<arrSQLTerms.length;s++) {
				String tablepath=arrSQLTerms[s]._strTableName;
				table mytable=tabledeserialize(tablepath);
				if (mytable!=null) {
					if (s==0){
						for(int i=0;i<mytable.pagesbucket.size();i++) {
							page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
							for(int j=0;j<mypage.vec.size();j++) {
								Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
								Enumeration<String> mykeys = myhashtable.keys();
								while(mykeys.hasMoreElements()) {
									String key=mykeys.nextElement();
									if(key.equals(arrSQLTerms[s]._strColumnName)){
if(arrSQLTerms[s]._objValue instanceof String) {
										
										arrSQLTerms[s]._objValue=convertStringToType((String)arrSQLTerms[s]._objValue,myhashtable.get(key).getClass().getName());
									}}
									if(key.equals(arrSQLTerms[s]._strColumnName) && whatop(arrSQLTerms[s]._strOperator,myhashtable.get(key),arrSQLTerms[s]._objValue)==true) {
										
										res.add(mypage.vec.get(j));
//										
										
									}
										}
								
							}
						mytable.pageserialize(mypage);}
					}
					else {
						
						 tempres.clear();
						for(int i=0;i<mytable.pagesbucket.size();i++) {
							
							page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
							for(int j=0;j<mypage.vec.size();j++) {
								Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
								Enumeration<String> mykeys = myhashtable.keys();
								while(mykeys.hasMoreElements()) {
									String key=mykeys.nextElement();
									String mytype=myhashtable.get(key).getClass().getName();
									if(key.equals(arrSQLTerms[s]._strColumnName)) {
									if(arrSQLTerms[s]._objValue instanceof String) {
										
										arrSQLTerms[s]._objValue=convertStringToType((String)arrSQLTerms[s]._objValue,myhashtable.get(key).getClass().getName());
									}}
									if(key.equals(arrSQLTerms[s]._strColumnName) && whatop(arrSQLTerms[s]._strOperator,myhashtable.get(key),arrSQLTerms[s]._objValue)==true) {
										boolean f=false;
										for(int t=0;t<tempres.size();t++) {
											if(areHashtablesEqual(tempres.get(t).getRowdata(),mypage.vec.get(j).getRowdata())) {
												f=true;
											}
										}
										if(f==false) {
										tempres.add(mypage.vec.get(j));}
										
									}
										}
								
							}
							mytable.pageserialize(mypage);}
						switch(strarrOperators[s-1]) {
						case "OR": 
							for(int t=0;t<tempres.size();t++) {
								
									boolean found=false;
									for(int r=0;r<res.size();r++) {
									if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
										if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
											found =true;
										}
									}
									
								}
									if(found==false) {
										res.add(tempres.get(t));
									}
							}
							break;
	
						case "AND":
							ArrayList<tuple> temp = new ArrayList<>();
							for(int t=0;t<tempres.size();t++) {
								
								boolean found=false;
								for(int r=0;r<res.size();r++) {
								if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
									if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
										found =true;
									}
								}
								
							}
								if(found==true) {
									temp.add(tempres.get(t));
								}
						}
							
							
							res.clear();
							res.addAll(temp);
							break;
							
						case "XOR":
							
							ArrayList<tuple> temp2 = new ArrayList<>();
							for(int t=0;t<tempres.size();t++) {
								
								boolean found=false;
								for(int r=0;r<res.size();r++) {
								if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
									if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
										found =true;
									}
								}
								
							}
								if(found==false) {
									temp2.add(tempres.get(t));
								}
						}
							
			
				for(int r=0;r<res.size();r++) {
								boolean found=false;
								for(int t=0;t<tempres.size();t++) {
								if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
									if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
										found =true;
									}
								}
								
							}
								if(found==false) {
									temp2.add(res.get(r));
								}
						}
							
				res.clear();
				res.addAll(temp2);
							break;
						}
					
					}
				tableserialize(mytable);
				
				}
				
				
			}
			System.out.println(res);
			return res.iterator();
		}
else {
	ArrayList<tuple> res = new ArrayList<>();
	String fileName = "metadata.csv";
 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
    File file = new File(filePath);
	
	if(arrSQLTerms.length%3==0) {
//		System.out.println("anad /3 f3lan");
		int mylengh=arrSQLTerms.length/3;
		boolean checkallindex=true;
	for(int i=0;i<arrSQLTerms.length;i++) {
		BufferedReader br=new BufferedReader(new FileReader(filePath));
		String line="";
		while((line=br.readLine())!=null) {
			String[]values=line.split(",");
			if(!values[5].equals("Octree")) {
				checkallindex=false;}
			}
	}
	if(checkallindex==true) {
		Vector<SQLTerm> temp=new Vector<SQLTerm>();
		for(int s=0;s<arrSQLTerms.length;s++) {
			temp.add(arrSQLTerms[s]);
		}
		
		Vector<String> vecoctreename=new Vector<String>();
		Vector<String> vectablename=new Vector<String>();
		while(!temp.isEmpty()) {
			int count=0;
			String tablename=temp.get(0)._strTableName;
			String treename=null;
			BufferedReader br=new BufferedReader(new FileReader(filePath));
			String line="";
			while((line=br.readLine())!=null) {
				String[]values=line.split(",");
				if(values[0].equals(tablename) && values[1].equals(temp.get(0)._strColumnName) && values[5].equals("Octree")) {
					treename=values[4];
					}
				}
			for(int i=0;i<temp.size();i++) {
				BufferedReader br2=new BufferedReader(new FileReader(filePath));
				String line2="";
				while((line=br2.readLine())!=null) {
					String[]values=line.split(",");
					if(values[0].equals(tablename) && values[1].equals(temp.get(i)._strColumnName) && values[5].equals("Octree") && values[4].equals(treename) ) {
						count ++;
						temp.remove(i);
						i--;
					}
						
						}
			}
			if(count==3) {
				vecoctreename.add(treename);
				vectablename.add(tablename);
			}
			
		}
		if(vecoctreename.size()==mylengh) {
//			System.out.println("ana f3lan index");
			ArrayList<tuple> indexres = new ArrayList<>();
			for(int k=0;k<vecoctreename.size();k++) {
				table mytable=tabledeserialize(vectablename.get(k));
				Octree myoctree=mytable.octreedeserialize(vecoctreename.get(k));
				String x=myoctree.col1;
				String y=myoctree.col2;
				String z=myoctree.col3;
				Object xvalue=null;
				Object yvalue=null;
				Object zvalue=null;
				String xop=null;
				String yop=null;
				String zop=null;
				
				for(int g=0;g<arrSQLTerms.length;g++) {
					if(arrSQLTerms[g]._strTableName.equals(vectablename.get(k)) &&arrSQLTerms[g]._strColumnName.equals(x)) {
						xvalue=arrSQLTerms[g]._objValue;
						xop=arrSQLTerms[g]._strOperator;
					}
					if(arrSQLTerms[g]._strTableName.equals(vectablename.get(k)) &&arrSQLTerms[g]._strColumnName.equals(y)) {
						yvalue=arrSQLTerms[g]._objValue;
						yop=arrSQLTerms[g]._strOperator;
					}
					if(arrSQLTerms[g]._strTableName.equals(vectablename.get(k)) &&arrSQLTerms[g]._strColumnName.equals(z)) {
						zvalue=arrSQLTerms[g]._objValue;
						zop=arrSQLTerms[g]._strOperator;
					}
				}
				List <String> pages=myoctree.searchByColumnsop(myoctree.root, x, y, z, xvalue, yvalue, zvalue, "AND", "AND", "AND");
				for(int p=0;p<pages.size();p++) {
			    	page mypage= mytable.pagedeserialize(pages.get(p));
			    	for(int t=0;t<mypage.vec.size();t++) {
			    		int found=0;
			    		Hashtable<String,Object> myhash=mypage.vec.get(t).getRowdata();
			    		Enumeration<String> mykeys = myhash.keys();
						while(mykeys.hasMoreElements()) {
							String key=mykeys.nextElement();
							if(key.equals(x)) {
							if(xvalue instanceof String) {
								
								xvalue=convertStringToType((String)xvalue,myhash.get(key).getClass().getName());
							}}
							if((key.equals(y))){
if(yvalue instanceof String) {
								
								yvalue=convertStringToType((String)yvalue,myhash.get(key).getClass().getName());
							}}
							if((key.equals(z))){
if(zvalue instanceof String) {
	
	zvalue=convertStringToType((String)zvalue,myhash.get(key).getClass().getName());
}}
							if((key.equals(x) && whatop(xop,myhash.get(key),xvalue)) 
									|| (key.equals(y) && whatop(yop,myhash.get(key),yvalue))
											||(key.equals(z) && whatop(zop,myhash.get(key),zvalue))) {
								found++;
							}
							}
						
			    	if(found==3) {
			    		indexres.add(mypage.vec.get(t));
			    	}
			    	}
			    	
			    mytable.pageserialize(mypage); }
			if(res.isEmpty()) {
				res.clear();
				res.addAll(indexres);}
			else {
				ArrayList<tuple> temp2 = new ArrayList<>();
				for(int t=0;t<indexres.size();t++) {
					
					boolean found=false;
					for(int r=0;r<res.size();r++) {
					if(compareObjects(res.get(r).getPrimkey(),indexres.get(t).getPrimkey())==0) {
						if(areHashtablesEqual(res.get(r).getRowdata(),indexres.get(t).getRowdata())) {
							found =true;
						}
					}
					
				}
					if(found==true) {
						temp2.add(indexres.get(t));
					}
			}
				res.clear();
				res.addAll(temp2);
			}
			mytable.octreeserialize(myoctree);
			tableserialize(mytable);
			}
			System.out.println(res);
			return res.iterator();}
		else {
			System.out.println(andindex(arrSQLTerms));
			return andindex(arrSQLTerms).iterator();
		}
			
		
		
	}
	else {
		System.out.println(andindex(arrSQLTerms));
		return andindex(arrSQLTerms).iterator();
	}
	}
	else {
		System.out.println(andindex(arrSQLTerms));
		return andindex(arrSQLTerms).iterator();
	}
	

			
		}
		}
		
	
	
	else {
//		System.out.println("not size");
//		System.out.println("fadya");
		return Collections.emptyIterator();
	}
	
	
}

public ArrayList<tuple> andindex (SQLTerm[] s ) throws DBAppException{
	
	 table mytable =tabledeserialize(s[0]._strTableName);
	 ArrayList<tuple> res = new ArrayList<>();
	 ArrayList<tuple> temp3= new ArrayList<>();
	 
		for(int i=0;i<mytable.pagesbucket.size();i++) {
			
			page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
			for(int j=0;j<mypage.vec.size();j++) {
				Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
				Enumeration<String> mykeys = myhashtable.keys();
				while(mykeys.hasMoreElements()) {
					String key=mykeys.nextElement();
					if(key.equals(s[0]._strColumnName)){
					if(s[0]._objValue instanceof String) {
						
						s[0]._objValue=convertStringToType((String)s[0]._objValue,myhashtable.get(key).getClass().getName());
					}}
					if(key.equals(s[0]._strColumnName) && whatop(s[0]._strOperator,myhashtable.get(key),s[0]._objValue)==true) {
						res.add(mypage.vec.get(j));
						
					}
						}
				
			}
			mytable.pageserialize(mypage);}
		tableserialize(mytable);


for(int t=1;t<s.length;t++) {
	
	
	  mytable =tabledeserialize(s[t]._strTableName);
	 ArrayList<tuple> tempres = new ArrayList<>();
		for(int i=0;i<mytable.pagesbucket.size();i++) {
			
			page mypage2=mytable.pagedeserialize(mytable.pagesbucket.get(i));
			for(int j=0;j<mypage2.vec.size();j++) {
				Hashtable<String, Object> myhashtable2=mypage2.vec.get(j).getRowdata();
				Enumeration<String> mykeys2 = myhashtable2.keys();
				while(mykeys2.hasMoreElements()) {
					String key2=mykeys2.nextElement();
					if(key2.equals(s[t]._strColumnName) ) {
					if(s[t]._objValue instanceof String) {
						
						s[t]._objValue=convertStringToType((String)s[t]._objValue,myhashtable2.get(key2).getClass().getName());
					}}
					if(key2.equals(s[t]._strColumnName) && whatop(s[t]._strOperator,myhashtable2.get(key2),s[t]._objValue)==true) {
						tempres.add(mypage2.vec.get(j));
						
					}
						}
				
			}
			mytable.pageserialize(mypage2);
			
			ArrayList<tuple> temp = new ArrayList<>();
			for(int z=0;z<tempres.size();z++) {
				
				boolean found=false;
				for(int r=0;r<res.size();r++) {
				if(compareObjects(res.get(r).getPrimkey(),tempres.get(z).getPrimkey())==0) {
					if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(z).getRowdata())) {
						found =true;
					}
				}
				
			}
				if(found==true) {
					temp.add(tempres.get(z));
				}
		}
			res.clear();
			res.addAll(temp);
			mytable.pageserialize(mypage2);}tableserialize(mytable);}
	

return res;
}


//public ArrayList<tuple> selecthelper(Vector<SQLTerm> arrSQLTerms,
//Vector<String> strarrOperators)
//throws DBAppException, IOException{
//	ArrayList<tuple> res = new ArrayList<>();
//	ArrayList<tuple> tempres = new ArrayList<>();
//	for (int s=0;s<arrSQLTerms.size();s++) {
//		String tablepath=arrSQLTerms.get(s)._strTableName;
//		table mytable=tabledeserialize(tablepath);
//		if (mytable!=null) {
//			if (s==0){
//				for(int i=0;i<mytable.pagesbucket.size();i++) {
//					page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
//					for(int j=0;j<mypage.vec.size();j++) {
//						Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
//						Enumeration<String> mykeys = myhashtable.keys();
//						while(mykeys.hasMoreElements()) {
//							String key=mykeys.nextElement();
//							if(key.equals(arrSQLTerms.get(s)._strColumnName) && whatop(arrSQLTerms.get(s)._strOperator,myhashtable.get(key),arrSQLTerms.get(s)._objValue)==true) {
//								
//								res.add(mypage.vec.get(j));
////								
//								
//							}
//								}
//						
//					}
//				mytable.pageserialize(mypage);}
//			}
//			else {
//				
//				 
//				for(int i=0;i<mytable.pagesbucket.size();i++) {
//					
//					page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
//					for(int j=0;j<mypage.vec.size();j++) {
//						Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
//						Enumeration<String> mykeys = myhashtable.keys();
//						while(mykeys.hasMoreElements()) {
//							String key=mykeys.nextElement();
//							String mytype=myhashtable.get(key).getClass().getName();
//							
//							if(key.equals(arrSQLTerms.get(s)._strColumnName) && whatop(arrSQLTerms.get(s)._strOperator,myhashtable.get(key),arrSQLTerms.get(s)._objValue)==true) {
//								tempres.add(mypage.vec.get(j));
//								
//							}
//								}
//						
//					}
//					mytable.pageserialize(mypage);}
//				switch(strarrOperators.get(s-1)) {
//				case "OR": 
//					for(int t=0;t<tempres.size();t++) {
//						
//							boolean found=false;
//							for(int r=0;r<res.size();r++) {
//							if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
//								if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
//									found =true;
//								}
//							}
//							
//						}
//							if(found==false) {
//								res.add(tempres.get(t));
//							}
//					}
//break;
//				case "AND":
//					ArrayList<tuple> temp = new ArrayList<>();
//					for(int t=0;t<tempres.size();t++) {
//						
//						boolean found=false;
//						for(int r=0;r<res.size();r++) {
//						if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
//							if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
//								found =true;
//							}
//						}
//						
//					}
//						if(found==true) {
//							temp.add(tempres.get(t));
//						}
//				}
//					res=temp;
//					break;
//					
//				case "XOR":
//					
//					ArrayList<tuple> temp2 = new ArrayList<>();
//					for(int t=0;t<tempres.size();t++) {
//						
//						boolean found=false;
//						for(int r=0;r<res.size();r++) {
//						if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
//							if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
//								found =true;
//							}
//						}
//						
//					}
//						if(found==false) {
//							temp2.add(tempres.get(t));
//						}
//				}
//					
//	
//		for(int r=0;r<res.size();r++) {
//						boolean found=false;
//						for(int t=0;t<tempres.size();t++) {
//						if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
//							if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
//								found =true;
//							}
//						}
//						
//					}
//						if(found==false) {
//							temp2.add(res.get(r));
//						}
//				}
//					
//					res=temp2;
//					break;
//				}
//			
//			}
//		tableserialize(mytable);
//		
//		}
//		
//		
//	}
//	System.out.println(res);
//	return res;
//}
public ArrayList<tuple> makeop (String op , ArrayList<tuple> res , ArrayList<tuple> tempres){
	switch(op) {
	case "OR": 
		for(int t=0;t<tempres.size();t++) {
			
				boolean found=false;
				for(int r=0;r<res.size();r++) {
				if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
					if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
						found =true;
					}
				}
				
			}
				if(found==false) {
					res.add(tempres.get(t));
				}
		}
break;
	case "AND":
		ArrayList<tuple> temp = new ArrayList<>();
		for(int t=0;t<tempres.size();t++) {
			
			boolean found=false;
			for(int r=0;r<res.size();r++) {
			if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
				if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
					found =true;
				}
			}
			
		}
			if(found==true) {
				temp.add(tempres.get(t));
			}
	}
		res.clear();
		res.addAll(temp);
		break;
		
	case "XOR":
		
		ArrayList<tuple> temp2 = new ArrayList<>();
		for(int t=0;t<tempres.size();t++) {
			
			boolean found=false;
			for(int r=0;r<res.size();r++) {
			if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
				if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
					found =true;
				}
			}
			
		}
			if(found==false) {
				temp2.add(tempres.get(t));
			}
	}
		

for(int r=0;r<res.size();r++) {
			boolean found=false;
			for(int t=0;t<tempres.size();t++) {
			if(compareObjects(res.get(r).getPrimkey(),tempres.get(t).getPrimkey())==0) {
				if(areHashtablesEqual(res.get(r).getRowdata(),tempres.get(t).getRowdata())) {
					found =true;
				}
			}
			
		}
			if(found==false) {
				temp2.add(res.get(r));
			}
	}
		
res.clear();
res.addAll(temp2);
		break;
	}
	return res;
}
public static boolean areHashtablesEqual(Hashtable<String, Object> hashtable1, Hashtable<String, Object> hashtable2) {
    if (hashtable1.size() != hashtable2.size()) {
        return false;
    }
    
    for (String key : hashtable1.keySet()) {
        if (!hashtable2.containsKey(key)) {
            return false;
        }
        
        Object value1 = hashtable1.get(key);
        Object value2 = hashtable2.get(key);
        
        if (compareObjects(value1,value2)!=0) {
            return false;
        }
    }
    
    return true;
}
//	
//	Hashtable <String,tuple> res =new Hashtable <String,tuple>();
//	Vector<String> restable=new Vector<String>();
//	Vector<Object> restup=new Vector<Object>();
//	
//	for (int s=0;s<arrSQLTerms.length;s++) {
//		
//	String tablepath=arrSQLTerms[s]._strTableName;
//	table mytable=tabledeserialize(tablepath);
//	if (mytable!=null) {
//		if (s==0){
//			for(int i=0;i<mytable.pagesbucket.size();i++) {
//				page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
//				for(int j=0;j<mypage.vec.size();j++) {
//					Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
//					Enumeration<String> mykeys = myhashtable.keys();
//					while(mykeys.hasMoreElements()) {
//						String key=mykeys.nextElement();
//						if(key.equals(arrSQLTerms[s]._strColumnName) && whatop(arrSQLTerms[s]._strOperator,myhashtable.get(key),arrSQLTerms[s]._objValue)==true) {
//							restable.add(arrSQLTerms[s]._strTableName);
//							restup.add(mypage.vec.get(j));
////							res.put(arrSQLTerms[s]._strTableName,mypage.vec.get(j));
//							
//						}
//							}
//					
//				}
//			mytable.pageserialize(mypage);}
//		}
//		else {
//			Hashtable <String,tuple> tempvec =new Hashtable <String,tuple>();
//			Vector<String> tempvec=new Vector<String>();
//			Vector<Object> tempvec=new Vector<Object>();
//			 
//			for(int i=0;i<mytable.pagesbucket.size();i++) {
//				
//				page mypage=mytable.pagedeserialize(mytable.pagesbucket.get(i));
//				for(int j=0;j<mypage.vec.size();j++) {
//					Hashtable<String, Object> myhashtable=mypage.vec.get(j).getRowdata();
//					Enumeration<String> mykeys = myhashtable.keys();
//					while(mykeys.hasMoreElements()) {
//						String key=mykeys.nextElement();
//						if(key.equals(arrSQLTerms[s]._strColumnName) && whatop(arrSQLTerms[s]._strOperator,myhashtable.get(key),arrSQLTerms[s]._objValue)==true) {
//							tempvec.put(arrSQLTerms[s]._strTableName,mypage.vec.get(j));
//							
//						}
//							}
//					
//				}
//				mytable.pageserialize(mypage);}
//			switch(strarrOperators[s-1]) {
//			case "OR": 
//				Enumeration<String> temkeys = tempvec.keys();
//				
//				while(temkeys.hasMoreElements()) {
//				    String tempkey = temkeys.nextElement();
//				Enumeration<String> reskeys = res.keys();
//				while(reskeys.hasMoreElements()) {
//				    String reskey = reskeys.nextElement();
//				    if(!(tempkey.equals(reskey) && compareObjects(tempvec.get(tempkey).getPrimkey(),res.get(reskey).getPrimkey())==0)) {
//				    	
//				    	res.put(tempkey,tempvec.get(tempkey))	;
//				    	
//				    }
//				}
//				
//			
//			}
//				break;
//			case "AND":
//				Hashtable <String,tuple> temphash =new Hashtable <String,tuple>();
//				 temkeys = tempvec.keys();
//				while(temkeys.hasMoreElements()) {
//				    String tempkey = temkeys.nextElement();
//				Enumeration<String> reskeys = res.keys();
//				while(reskeys.hasMoreElements()) {
//				    String reskey = reskeys.nextElement();
//				    if(tempkey.equals(reskey) && compareObjects(tempvec.get(tempkey).getPrimkey(),res.get(reskey).getPrimkey())==0) {
//				    	
//				    	temphash.put(tempkey,tempvec.get(tempkey))	;
//				    	
//				    }
//				}
//				
//			
//			} res=temphash;
//			break;
//			case "XOR":
//				 temphash =new Hashtable <String,tuple>();
//				 temkeys = tempvec.keys();
//				while(temkeys.hasMoreElements()) {
//				    String tempkey = temkeys.nextElement();
//				Enumeration<String> reskeys = res.keys();
//				while(reskeys.hasMoreElements()) {
//				    String reskey = reskeys.nextElement();
//				    if(!(tempkey.equals(reskey) && compareObjects(tempvec.get(tempkey).getPrimkey(),res.get(reskey).getPrimkey())==0)) {
//				    	
//				    	temphash.put(tempkey,tempvec.get(tempkey))	;
//				    	
//				    }
//				}
//				
//			
//			}
//				 temkeys = res.keys();
//					while(temkeys.hasMoreElements()) {
//					    String tempkey = temkeys.nextElement();
//					Enumeration<String> reskeys = tempvec.keys();
//					while(reskeys.hasMoreElements()) {
//					    String reskey = reskeys.nextElement();
//					    if(!(tempkey.equals(reskey) && compareObjects(tempvec.get(tempkey).getPrimkey(),res.get(reskey).getPrimkey())==0)) {
//					    	
//					    	temphash.put(tempkey,tempvec.get(tempkey))	;
//					    	
//					    }
//					}
//					
//				
//				}
//					res=temphash;
//					break;
//			}
//		}
//		
//	}
//	else {
//		 throw new DBAppException();
////		 return Collections.emptyIterator();
//	}
//	tableserialize(mytable);}
//	Vector <tuple> resvec=new Vector <tuple>();
//	Enumeration<String> reskeys = res.keys();
//	while(reskeys.hasMoreElements()) {
//		String reskey = reskeys.nextElement();
//		resvec.add(res.get(reskey));
//	}
//	Enumeration<String> reskeys2 = res.keys();
//	while(reskeys2.hasMoreElements()) {
//		String reskey = reskeys2.nextElement();
//		System.out.println(reskey);
//		System.out.println(res.get(reskey).getRowdata());
//	}
//	return resvec.iterator();
//
//	
//	
//	return null;
//}
	    
private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
	 String fileName = "courses_table.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
    BufferedReader coursesTable = new BufferedReader(new FileReader(filePath));
    String record;
    Hashtable<String, Object> row = new Hashtable<>();
    int c = limit;
    if (limit == -1) {
        c = 1;
    }
    while ((record = coursesTable.readLine()) != null && c > 0) {
        String[] fields = record.split(",");


        int year = Integer.parseInt(fields[0].trim().substring(0, 4));
        int month = Integer.parseInt(fields[0].trim().substring(5, 7));
        int day = Integer.parseInt(fields[0].trim().substring(8));

        Date dateAdded = new Date(year - 1900, month - 1, day);

        row.put("date_added", dateAdded);

        row.put("course_id", fields[1]);
        row.put("course_name", fields[2]);
        row.put("hours", Integer.parseInt(fields[3]));

        dbApp.insertIntoTable("courses", row);
        row.clear();

        if (limit != -1) {
            c--;
        }
    }

    coursesTable.close();
}
private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
	 String fileName = "students_table.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
    BufferedReader studentsTable = new BufferedReader(new FileReader(filePath));
    String record;
    int c = limit;
    if (limit == -1) {
        c = 1;
    }

    Hashtable<String, Object> row = new Hashtable<>();
    while ((record = studentsTable.readLine()) != null && c > 0) {
        String[] fields = record.split(",");

        row.put("id", fields[0]);
        row.put("first_name", fields[1]);
        row.put("last_name", fields[2]);

        int year = Integer.parseInt(fields[3].trim().substring(0, 4));
        int month = Integer.parseInt(fields[3].trim().substring(5, 7));
        int day = Integer.parseInt(fields[3].trim().substring(8));

        Date dob = new Date(year - 1900, month - 1, day);
        row.put("dob", dob);

        double gpa = Double.parseDouble(fields[4].trim());

        row.put("gpa", gpa);

        dbApp.insertIntoTable("students", row);
        row.clear();
        if (limit != -1) {
            c--;
        }
    }
    studentsTable.close();
}
private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
	 String fileName = "transcripts_table.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
 BufferedReader transcriptsTable = new BufferedReader(new FileReader(filePath));
    String record;
    Hashtable<String, Object> row = new Hashtable<>();
    int c = limit;
    if (limit == -1) {
        c = 1;
    }
    while ((record = transcriptsTable.readLine()) != null && c > 0) {
        String[] fields = record.split(",");

        row.put("gpa", Double.parseDouble(fields[0].trim()));
        row.put("student_id", fields[1].trim());
        row.put("course_name", fields[2].trim());

        String date = fields[3].trim();
        int year = Integer.parseInt(date.substring(0, 4));
        int month = Integer.parseInt(date.substring(5, 7));
        int day = Integer.parseInt(date.substring(8));

        Date dateUsed = new Date(year - 1900, month - 1, day);
        row.put("date_passed", dateUsed);

        dbApp.insertIntoTable("transcripts", row);
        row.clear();

        if (limit != -1) {
            c--;
        }
    }

    transcriptsTable.close();
}
private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
	 String fileName = "pcs_table.csv";
	 	String filePath = System.getProperty("user.dir") + "/resources/" + fileName;
    BufferedReader pcsTable = new BufferedReader(new FileReader(filePath));
    String record;
    Hashtable<String, Object> row = new Hashtable<>();
    int c = limit;
    if (limit == -1) {
        c = 1;
    }
    while ((record = pcsTable.readLine()) != null && c > 0) {
        String[] fields = record.split(",");

        row.put("pc_id", Integer.parseInt(fields[0].trim()));
        row.put("student_id", fields[1].trim());

        dbApp.insertIntoTable("pcs", row);
        row.clear();

        if (limit != -1) {
            c--;
        }
    }

    pcsTable.close();
}
private static void createTranscriptsTable(DBApp dbApp) throws Exception {
    // Double CK
    String tableName = "transcripts";

    Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
    htblColNameType.put("gpa", "java.lang.Double");
    htblColNameType.put("student_id", "java.lang.String");
    htblColNameType.put("course_name", "java.lang.String");
    htblColNameType.put("date_passed", "java.util.Date");

    Hashtable<String, String> minValues = new Hashtable<>();
    minValues.put("gpa", "0.7");
    minValues.put("student_id", "43-0000");
    minValues.put("course_name", "AAAAAA");
    minValues.put("date_passed", "1990-01-01");

    Hashtable<String, String> maxValues = new Hashtable<>();
    maxValues.put("gpa", "5.0");
    maxValues.put("student_id", "99-9999");
    maxValues.put("course_name", "zzzzzz");
    maxValues.put("date_passed", "2020-12-31");

    dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
}
private static void createStudentTable(DBApp dbApp) throws Exception {
    // String CK
    String tableName = "students";

    Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
    htblColNameType.put("id", "java.lang.String");
    htblColNameType.put("first_name", "java.lang.String");
    htblColNameType.put("last_name", "java.lang.String");
    htblColNameType.put("dob", "java.util.Date");
    htblColNameType.put("gpa", "java.lang.Double");

    Hashtable<String, String> minValues = new Hashtable<>();
    minValues.put("id", "43-0000");
    minValues.put("first_name", "AAAAAA");
    minValues.put("last_name", "AAAAAA");
    minValues.put("dob", "1990-01-01");
    minValues.put("gpa", "0.7");

    Hashtable<String, String> maxValues = new Hashtable<>();
    maxValues.put("id", "99-9999");
    maxValues.put("first_name", "zzzzzz");
    maxValues.put("last_name", "zzzzzz");
    maxValues.put("dob", "2000-12-31");
    maxValues.put("gpa", "5.0");

    dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
}
private static void createPCsTable(DBApp dbApp) throws Exception {
    // Integer CK
    String tableName = "pcs";

    Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
    htblColNameType.put("pc_id", "java.lang.Integer");
    htblColNameType.put("student_id", "java.lang.String");


    Hashtable<String, String> minValues = new Hashtable<>();
    minValues.put("pc_id", "0");
    minValues.put("student_id", "43-0000");

    Hashtable<String, String> maxValues = new Hashtable<>();
    maxValues.put("pc_id", "20000");
    maxValues.put("student_id", "99-9999");

    dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
}
private static void createCoursesTable(DBApp dbApp) throws Exception {
    // Date CK
    String tableName = "courses";

    Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
    htblColNameType.put("date_added", "java.util.Date");
    htblColNameType.put("course_id", "java.lang.String");
    htblColNameType.put("course_name", "java.lang.String");
    htblColNameType.put("hours", "java.lang.Integer");


    Hashtable<String, String> minValues = new Hashtable<>();
    minValues.put("date_added", "1901-01-01");
    minValues.put("course_id", "0000");
    minValues.put("course_name", "AAAAAA");
    minValues.put("hours", "1");

    Hashtable<String, String> maxValues = new Hashtable<>();
    maxValues.put("date_added", "2020-12-31");
    maxValues.put("course_id", "9999");
    maxValues.put("course_name", "zzzzzz");
    maxValues.put("hours", "24");

    dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

}
	
	public static void main(String[] args) throws Exception {
		 
	     
	DBApp db=new DBApp();
//	 createCoursesTable(db);
//     createPCsTable(db);
//     createTranscriptsTable(db);
//     createStudentTable(db);
//     insertPCsRecords(db,200);
//     insertTranscriptsRecords(db,200);
//     insertStudentRecords(db,200);
//     insertCoursesRecords(db,200);
//	printTableContents("transcripts");
//	 String table = "students";
//     Hashtable<String, Object> row = new Hashtable();
//     row.put("id", 123);
//     
//     row.put("first_name", "foo");
//     row.put("last_name", "bar");
//
//     Date dob = new Date(1995 - 1900, 4 - 1, 1);
//     row.put("dob", dob);
//     row.put("gpa", 1.1);
//     db.insertIntoTable(table, row);
//	  String table = "transcripts";
//      Hashtable<String, Object> row = new Hashtable();
//      row.put("gpa", 1.5);
//      row.put("student_id", "34-9874");
//      row.put("course_name", "bar");
//      row.put("elective", true);


//      Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
//      row.put("date_passed", date_passed);
//      db.insertIntoTable(table, row);
//    String table = "students";

//      row.put("first_name", "nora");
//      row.put("gpa", 4.34);

//      Date dob = new Date(1992 - 1900, 9 - 1, 8);
//      row.put("dob", dob);
//      row.put("gpa", 1.1);
//      row.put("os", 1.1);
//row.put(, value)
//      db.updateTable(table, "47-2285", row);
//      db.deleteFromTable(table, row);
//      printTableContents("students");
      
//	d.tabledeserialize("t1");
//		printTableContents("table1");

//  Hashtable<String, String> httype = new Hashtable<String, String>();
//  httype.put("name","java.lang.String");
//  httype.put("id","java.lang.Integer");
//  httype.put("age", "java.lang.Integer");
//  httype.put("date","java.util.Date");
//  Hashtable<String, String> htmin = new Hashtable<String, String>();
//  htmin.put("name","A");
//  htmin.put("id","1");
//  htmin.put("age","1");
//  htmin.put("date","1990-06-06");
//  Hashtable<String, String> htm = new Hashtable<String, String>();
//  htm.put("name","ZZZZZZ");
//  htm.put("id","1000000");
//  htm.put("age","10000000");
//  htm.put("date","2030-06-06");
//db.createTable("ta3", "id", httype, htmin, htm);
////  
////
	 Date dob = new Date(1995 - 1900, 4 - 1, 1);
//     
//  Hashtable<String, Object> htmax = new Hashtable<String, Object>();
//  htmax.put("first_name","samarattt");
//  htmax.put("last_name","pima");
//  htmax.put("id","44-10");
//  htmax.put("gpa",4.6);
//  htmax.put("dob", dob);
// db.insertIntoTable("students", htmax);
// db.printTableContents("students");
//
//	d.createTable("ta3","id",httype,htmin,htmax);
//	String [] mmm={"first_name","last_name","gpa"};
//	db.createIndex("students",mmm);
//	
//	db.updateTable("students","44-6", htmax);
	
//	d.createTable("table18","id",httype,htmin,htmax);			
////	System.out.println( httype.size());
//	
	
//	d.init();
//	d.init();
//	d.createTable("table11","id",httype,htmin,htmax);
//	d.createTable("table 11","id",httype,htmin,htmax);			
//	System.out.println( httype.size());
	

//		File file = new File(fileUrl.getPath().replace("bin", "resources").replace("/", "\\\\").replaceFirst("\\\\\\\\", "").toString());
//	    PrintWriter p=new PrintWriter(file);
//	    
//	    p.write("table1,id, int,True, IndexName1, IndexType1, min1, max1");
//	    p.write("\ntable1,name, int, false, IndexName1, IndexType1, min1, max1");
//	    p.write("\ntable2,id, int,True, IndexName2, IndexType2, min2, max2");
//         p.close();
//		
////         
//      Hashtable<String, Object> ht = new Hashtable<String, Object>();
// 		ht.put("name","Andrew");
// 		ht.put("id",4);
// 	    ht.put("age", 20);
// 		ht.put("date", "2020-06-06");
// 		System.out.println(ht);
//	
//     d.insertIntoTable("t45", ht);
	
	




	
////	
//	Hashtable<String, Object> ht5 = new Hashtable<String, Object>();
//ht5.put("name","Andrew");
//ht5.put("age","20");
//ht5.put("id", 4);
//System.out.println(ht5);	
//d.deleteFromTable("t45", ht5);
//printTableContents("t45");


	

//		
//		 File csvFile = new File("C:\\Users\\hp\\OneDrive\\Desktop\\m2\\database2\\resources\\metadata.csv");
//	        
//	        try {
//	            // Open the file in write mode using a FileWriter
//	            FileWriter fileWriter = new FileWriter(csvFile);
//	            
//	            // Write an empty string to the file
//	            fileWriter.write("");
//	            
//	            // Close the file writer
//	            fileWriter.close();
//	            
//	            System.out.println("All information deleted from the CSV file.");
//	        } catch (IOException e) {
//	            System.out.println("An error occurred while deleting information from the CSV file: " + e.getMessage());
//	        }
//
//Hashtable<String, Object> ht6 = new Hashtable<String, Object>();
//ht6.put("name","Cndrew");
////ht6.put("date","2002-06-06");
//d.updateTable("t45","2",ht6);
//printTableContents("t45");
	
////	
			
				
	

//  Hashtable<String, Object> ht = new Hashtable<String, Object>();
//	ht.put("name","Andrew");
//	ht.put("id",2);
//	System.out.println(ht);
//	boolean b1=validate("table1",ht);
//	System.out.println(b1);
	
	
	
//	Enumeration<String> mykeys = ht.keys();
//	tuple newtuple=null;
//	
//	while(mykeys.hasMoreElements()) {
//		String key=mykeys.nextElement();
//	System.out.println(key);
//	System.out.println(ht.get(key));
//	System.out.println(ht.get(key).getClass().getName());
//	String line="";
//	URL fileUrl = getsource.getCsvFileSource("metadata.csv");
//	BufferedReader br=new BufferedReader(new FileReader(fileUrl.getPath().replace("bin", "resources").replace("/", "\\\\").replaceFirst("\\\\\\\\", "").toString()));
//	
//	while((line=br.readLine())!=null) {
//		
//		String[]values=line.split(",");
//		System.out.println(values[0]);
//		System.out.println(values[1]);
//		System.out.println(values[2]);
//		if(values[0].equals("table1") && values[1].equals(key)) {
//			System.out.println("mahy sha8ala ahu");
//		}
//		
//	}
//	
//}
 
	SQLTerm[] arrSQLTerms;
	arrSQLTerms = new SQLTerm[3];
	arrSQLTerms[0] = new SQLTerm();
	arrSQLTerms[0]._strTableName = "students";
	arrSQLTerms[0]._strColumnName= "first_name";
	arrSQLTerms[0]._strOperator = "<=";
	arrSQLTerms[0]._objValue = "xxxxxxxx";
	
	
	arrSQLTerms[1] = new SQLTerm();
	arrSQLTerms[1]._strTableName = "students";
	arrSQLTerms[1]._strColumnName= "gpa";
	arrSQLTerms[1]._strOperator = "<=";
	arrSQLTerms[1]._objValue = 1.8;
	arrSQLTerms[2] = new SQLTerm();
	arrSQLTerms[2]._strTableName = "students";
	arrSQLTerms[2]._strColumnName= "last_name";
	arrSQLTerms[2]._strOperator = "<";
	arrSQLTerms[2]._objValue = "xxxxxx";
//	arrSQLTerms[3] = new SQLTerm();
//	arrSQLTerms[3]._strTableName = "ta2";
//	arrSQLTerms[3]._strColumnName= "date";
//	arrSQLTerms[3]._strOperator = ">";
//	arrSQLTerms[3]._objValue ="2000-01-01";
	String[]strarrOperators = new String[2];
	strarrOperators[0] = "AND";
	strarrOperators[1] = "AND";
//	strarrOperators[2] = "OR";
Iterator it=db.selectFromTable(arrSQLTerms, strarrOperators);
while(it.hasNext()) {
	tuple t=(tuple) it.next();
	
	System.out.println(t.getRowdata());
}
 
//	String [] mmm={"name","age","id"};
//	d.createIndex("ta2",mmm);
//	printTableContents("ta2");
	
	
	
//	Hashtable<String, Object> ht1 = new Hashtable<String, Object>();
//		ht1.put("name","Andrew");
//		ht1.put("id",1);
//	    ht1.put("age", 15);
//		ht1.put("date", "2006-12-02");
//		System.out.println(ht1);
//
// d.insertIntoTable("ta1", ht1);
//printTableContents("ta1");
//
//
//Hashtable<String, Object> ht2 = new Hashtable<String, Object>();
//		ht2.put("name","Gera");
//		ht2.put("id",2);
//	    ht2.put("age", 14);
//		ht2.put("date", "2015-12-03");
//		System.out.println(ht2);
//
// d.insertIntoTable("ta1", ht2);
//printTableContents("ta1");
//
//Hashtable<String, Object> ht3 = new Hashtable<String, Object>();
//		ht3.put("name","Youanna");
//		ht3.put("id",3);
//	    ht3.put("age", 16);
//		ht3.put("date", "2020-11-06");
//		System.out.println(ht3);
//
// d.insertIntoTable("ta1", ht3);
//printTableContents("ta1");
//
//Hashtable<String, Object> ht4 = new Hashtable<String, Object>();
//		ht4.put("name","Didi");
//		ht4.put("id",4);
//	    ht4.put("age", 14);
//		ht4.put("date", "2013-07-02");
//		System.out.println(ht4);
//
// d.insertIntoTable("ta1", ht4);
//printTableContents("ta1");
//
//
//
//Hashtable<String, Object> ht5 = new Hashtable<String, Object>();
//		ht5.put("name","Dodo");
//		ht5.put("id",5);
//	    ht5.put("age", 21);
//		ht5.put("date", "2016-01-08");
//		System.out.println(ht5);
//
// d.insertIntoTable("ta1", ht5);
//printTableContents("ta1");
//
//
//
//Hashtable<String, Object> ht6 = new Hashtable<String, Object>();
//		ht6.put("name","Andrew");
//		ht6.put("id",6);
//	    ht6.put("age", 14);
//		ht6.put("date", "2006-12-02");
//		System.out.println(ht6);
//
// d.insertIntoTable("ta1", ht6);
//printTableContents("ta1");
//
//
//Hashtable<String, Object> ht7 = new Hashtable<String, Object>();
//		ht7.put("name","Doom");
//		ht7.put("id",1);
//	    ht7.put("age", 15);
//		ht7.put("date", "2017-05-06");
//		System.out.println(ht7);
//
// d.insertIntoTable("ta2", ht7);
//printTableContents("ta2");
//

//Hashtable<String, Object> ht8 = new Hashtable<String, Object>();
//		ht8.put("name","Gera");
//		ht8.put("id",2);
//	    ht8.put("age", 14);
//		ht8.put("date", "2015-12-03");
//		System.out.println(ht8);
//
// d.insertIntoTable("ta2", ht8);
//printTableContents("ta2");
//
//Hashtable<String, Object> ht9 = new Hashtable<String, Object>();
//		ht9.put("name","Didi");
//		ht9.put("id",3);
//	    ht9.put("age", 20);
//		ht9.put("date", "2006-12-02");
//		System.out.println(ht9);
//
// d.insertIntoTable("ta2", ht9);
//printTableContents("ta2");
//
//
//Hashtable<String, Object> ht10 = new Hashtable<String, Object>();
//		ht10.put("name","Dodo");
//		ht10.put("id",5);
//	    ht10.put("age", 2);
//		ht10.put("date", "2016-01-08");
//		System.out.println(ht10);
//
// d.insertIntoTable("ta2", ht10);
//printTableContents("ta2");
//
//
//Hashtable<String, Object> ht11 = new Hashtable<String, Object>();
//		ht11.put("name","Yoyo ");
//		ht11.put("id",6);
//	    ht11.put("age", 16);
//		ht11.put("date", "2021-05-03");
//		System.out.println(ht11);
//
// d.insertIntoTable("ta2", ht11);
//printTableContents("ta2");
//
//
//
//Hashtable<String, Object> ht12 = new Hashtable<String, Object>();
//		ht12.put("name","Gera");
//		ht12.put("id",10);
//	    ht12.put("age", 14);
//		ht12.put("date", "2015-12-03");
//		System.out.println(ht12);
//
// d.insertIntoTable("ta2", ht12);
//printTableContents("ta2");
//
//
//
//Hashtable<String, Object> ht13 = new Hashtable<String, Object>();
//		ht13.put("name","Yoyo ");
//		ht13.put("id",6);
//	    ht13.put("age", 16);
//		ht13.put("date", "2021-05-03");
//		System.out.println(ht13);
//
// d.insertIntoTable("ta3", ht13);
//printTableContents("ta3");
//
//
//Hashtable<String, Object> ht14 = new Hashtable<String, Object>();
//		ht14.put("name","Youana ");
//		ht14.put("id",3);
//	    ht14.put("age", 12);
//		ht14.put("date", "2020-04-04");
//		System.out.println(ht14);
//
// d.insertIntoTable("ta3", ht14);
//printTableContents("ta3");
//
//
//
//Hashtable<String, Object> ht15 = new Hashtable<String, Object>();
//		ht15.put("name","Yiyi ");
//		ht15.put("id",6);
//	    ht15.put("age", 14);
//		ht15.put("date", "2020-11-06");
//		System.out.println(ht15);
//
// d.insertIntoTable("ta3", ht15);
//printTableContents("ta3");
//
//
//
//Hashtable<String, Object> ht16 = new Hashtable<String, Object>();
//		ht16.put("name","Gera ");
//		ht16.put("id",2);
//	    ht16.put("age", 12);
//		ht16.put("date", "2015-12-03");
//		System.out.println(ht16);
//
// d.insertIntoTable("ta3", ht16);
//printTableContents("ta3");
//
//
//
//Hashtable<String, Object> ht17 = new Hashtable<String, Object>();
//		ht17.put("name","Doom ");
//		ht17.put("id",1);
//	    ht17.put("age", 15);
//		ht17.put("date", "2017-05-06");
//		System.out.println(ht17);
//
// d.insertIntoTable("ta3", ht17);
//printTableContents("ta3");
//	printTableContents("ta1");
//	printTableContents("ta2");
//	printTableContents("ta3");

    }
}	
				
				
				
				
				
		
	
	
	
	
	
