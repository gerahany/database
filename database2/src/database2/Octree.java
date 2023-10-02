package database2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Octree implements Serializable{
	private static  int MAX_ELEMENTS;
	static {
		String fileName = "DBApp.config";
		 String filePath = System.getProperty("user.dir") + "/src/database2/" + fileName;
try (BufferedReader br = new BufferedReader(new FileReader(filePath))){
	String line;
    int lineCount = 0;
while((line = br.readLine()) != null) {
	lineCount++;
    if (lineCount == 2) { // Check if it's the second line
        String[] parts = line.split("=");
        if (parts.length == 2) {
            String valueString = parts[1].trim();
            int value = Integer.parseInt(valueString);
            System.out.println(value);
            MAX_ELEMENTS = value;
        }
        break; // Exit the loop after processing the second line
    }

}
}
catch(IOException e){
	e.printStackTrace();

}
	}

    public OctreeNode root;
    public String col1="";
    public String col2="";
    public String col3="";
    private String OctreePath;
    public void printAllNodes() {
        printAllNodesRecursive(root);
    }

    private void printAllNodesRecursive(OctreeNode node) {
        if(node==null) {
        	return;
        }
        System.out.println("Node Boundaries: (" + node.minX + ", " + node.maxX + "), (" +
                node.minY + ", " + node.maxY + "), (" + node.minZ + ", " + node.maxZ + ")");
        if(node.elements != null) {
        	for(OctreeElement element : node.elements) {
        		System.out.println("Element: (" + element.x + ", " + element.y + ", " + element.z + ")");
        	}
        }
        if(node.children != null) {
        	for(OctreeNode child : node.children) {
        		 printAllNodesRecursive(child);
        	}
        }
    }

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
    
    public Octree(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
        this.setRoot(new OctreeNode(minX, maxX, minY, maxY, minZ, maxZ));
    }

    public void insert(Object x, Object y, Object z, String reference) {
        getRoot().insert(x, y, z, reference);
    }

    public void remove(Object x, Object y, Object z) {
        removeElement(getRoot(), x, y, z);
    }

    private boolean removeElement(OctreeNode node, Object x, Object y, Object z) {
        if (node.children == null) {
            Iterator<OctreeElement> iterator = node.elements.iterator();
            while (iterator.hasNext()) {
                OctreeElement element = iterator.next();
                if (compareObjects(element.x, x) == 0 &&
                		compareObjects(element.y, y) == 0 &&
                				compareObjects(element.z, z) == 0) {
                    iterator.remove();
                    return true;
                }
            }
        } else {
            int index = node.getChildIndex(x, y, z);
            if (removeElement(node.children[index], x, y, z)) {
                // Check if the child node became empty after the removal
                if (node.children[index].isEmpty()) {
                    node.children[index] = null;
                }
                return true;
            }
        }
        return false;
    }

    public List<String> searchElements(OctreeNode node, Object x, Object y, Object z) {
        List<String> references = new ArrayList<>();
        searchElementsRecursive(node, x, y, z, references);
        return references;
    }

    public void searchElementsRecursive(OctreeNode node, Object x, Object y, Object z, List<String> references) {
        if (node.children == null) {
            for (OctreeElement element : node.elements) {
                if (( compareObjects(element.x, x) == 0) &&
                    ( compareObjects(element.y, y) == 0) &&
                    ( compareObjects(element.z, z) == 0)) {
                    references.add(element.reference);
                }
            }
        } else {
            int index = node.getChildIndex(x, y, z);
            if (node.children[index] != null) {
                searchElementsRecursive(node.children[index], x, y, z, references);
            }
        }
    }
    
    public List<String> searchByColumn(OctreeNode node, String columnName, Object columnValue) {
        List<String> references = new ArrayList<>();
        searchByColumnRecursive(node, columnName, columnValue, references);
        return references;
    }
    private void searchByColumnRecursive(OctreeNode node, String columnName, Object columnValue, List<String> references) {
        if (node.children == null) {
            for (OctreeElement element : node.elements) {
                Object value = getValueByColumnName(element, columnName);
                if (compareObjects(value, columnValue) == 0) {
                    references.add(element.reference);
                }
            }
        } else {
            for (OctreeNode child : node.children) {
                if (child != null) {
                    searchByColumnRecursive(child, columnName, columnValue, references);
                }
            }
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
    	break;}
    	
    	return false;
    }
    public List<String> searchByColumnsop(OctreeNode node, String col1Name, String col2Name, String col3Name, Object col1Value, Object col2Value, Object col3Value, String op1, String op2, String op3) {
        List<String> references = new ArrayList<>();
        Queue<OctreeNode> queue = new LinkedList<>();
        queue.add(node);

        while (!queue.isEmpty()) {
            OctreeNode currentNode = queue.poll();
            if (currentNode.children == null) {
                for (OctreeElement element : currentNode.elements) {
                    Object value1 = getValueByColumnName(element, col1Name);
                    Object value2 = getValueByColumnName(element, col2Name);
                    Object value3 = getValueByColumnName(element, col3Name);

                    if (whatop( op1,value1, col1Value) &&whatop( op2,value2, col2Value) && whatop( op3,value3, col3Value)) {
                        references.add(element.reference);
                    }
                }
            } else {
                for (OctreeNode child : currentNode.children) {
                    if (child != null) {
                        queue.add(child);
                    }
                }
            }
        }

        return references;
    }

    private Object getValueByColumnName(OctreeElement element, String columnName) {
      
        if(columnName.equals(col1)) {
        	 return element.x;
        }
        else if(columnName.equals(col2)) {
       	 return element.y;
       }
        else if(columnName.equals(col3)) {
       	 return element.z;
       }
        else { 
        	throw new IllegalArgumentException("Invalid column name: " + columnName);
        }
        
    }

    
    public OctreeNode getRoot() {
		return root;
	}

	public void setRoot(OctreeNode root) {
		this.root = root;
	}

	private static class OctreeElement  implements Serializable{
        private final Object x, y, z;
        private final String reference;

        public OctreeElement(Object x, Object y, Object z, String reference) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.reference = reference;
        }
    }
    
    private static class OctreeNode implements Serializable {
        private final Object minX, maxX, minY, maxY, minZ, maxZ;
        private List<OctreeElement> elements;
        private OctreeNode[] children;

        public OctreeNode(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.elements = new ArrayList<>();
            this.children = null;
        }
        
        public boolean isEmpty() {
            if (children == null) {
                return elements.isEmpty();
            } else {
                for (OctreeNode child : children) {
                    if (child != null && !child.isEmpty()) {
                        return false;
                    }
                }
                return true;
            }
        } 

        public void insert(Object x, Object y, Object z, String ref) {
            if (children == null) {
                if (elements.size() < MAX_ELEMENTS) {
                    elements.add(new OctreeElement(x, y, z, ref)); //reference of tuple
                } else {
                    split();
                    insert(x, y, z, ref);
                }
            } else {
                int index = getChildIndex(x, y, z);
                children[index].insert(x, y, z, ref);
            }
        }


        private void split() {
            children = new OctreeNode[8];
            Object midX = calculateMidpoint(minX , maxX);
            Object midY = calculateMidpoint(minY , maxY);
            Object midZ = calculateMidpoint(minZ , maxZ);
            children[0] = new OctreeNode(minX, midX, minY, midY, minZ, midZ);
            children[1] = new OctreeNode(midX, maxX, minY, midY, minZ, midZ);
            children[2] = new OctreeNode(minX, midX, midY, maxY, minZ, midZ);
            children[3] = new OctreeNode(midX, maxX, midY, maxY, minZ, midZ);
            children[4] = new OctreeNode(minX, midX, minY, midY, midZ, maxZ);
            children[5] = new OctreeNode(midX, maxX, minY, midY, midZ, maxZ);
            children[6] = new OctreeNode(minX, midX, midY, maxY, midZ, maxZ);
            children[7] = new OctreeNode(midX, maxX, midY, maxY, midZ, maxZ);

            for (OctreeElement element : elements) {
                int index = getChildIndex(element.x, element.y, element.z);
                children[index].insert(element.x, element.y, element.z, element.reference);
            }
            elements = null;
        }

        public int getChildIndex(Object x, Object y, Object z) {
            int index = 0;
            if (compareObj(x, calculateMidpoint(minX , maxX)) >= 0) index |= 1;
            if (compareObj(y, calculateMidpoint(minY , maxY) ) >= 0) index |= 2;
            if (compareObj(z, calculateMidpoint(minZ , maxZ) ) >= 0) index |= 4;
            return index;
        }

        public boolean containsPoint(Object x, Object y, Object z) {
            return compareObj(x, minX) >= 0 && compareObj(x, maxX) <= 0 &&
                   compareObj(y, minY) >= 0 && compareObj(y, maxY) <= 0 &&
                   compareObj(z, minZ) >= 0 && compareObj(z, maxZ) <= 0;
        }
        public static <T> T convertObject(Object obj, Class<T> targetType) {
            if (obj == null) {
                return null;
            }

            if (targetType.isInstance(obj)) {
                return targetType.cast(obj);
            }

            if (targetType == String.class) {
                return targetType.cast(obj.toString());
            }

            if (targetType == Integer.class || targetType == int.class) {
                if (obj instanceof Number) {
                    return targetType.cast(((Number) obj).intValue());
                } else {
                    try {
                        return targetType.cast(Integer.parseInt(obj.toString()));
                    } catch (NumberFormatException e) {
                        // Handle the exception if the object cannot be converted to an Integer
                    }
                }
            }

            if (targetType == Double.class || targetType == double.class) {
                if (obj instanceof Number) {
                    return targetType.cast(((Number) obj).doubleValue());
                } else {
                    try {
                        return targetType.cast(Double.parseDouble(obj.toString()));
                    } catch (NumberFormatException e) {
                        // Handle the exception if the object cannot be converted to a Double
                    }
                }
            }

            if (targetType == Date.class) {
                if (obj instanceof Date) {
                    return targetType.cast(obj);
                } else {
                    try {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        Date date = dateFormat.parse(obj.toString());
                        return targetType.cast(date);
                    } catch (ParseException e) {
                        // Handle the exception if the object cannot be parsed as a Date
                    }
                }
            }

            // If the object cannot be converted to the target type, return null or throw an exception
            return null;
        }
        public static int compareObj(Object o1, Object o2) {
        	 String currDataType = o1.getClass().getName();
        	    int result;

        	    switch (currDataType) {
        	        case "java.lang.Integer":
        	            if (o2 instanceof String) {
        	                o2 = Integer.parseInt((String) o2);
        	            }
        	            result = Integer.compare((int) o1, (int) o2);
        	            break;

        	        case "java.lang.String":
        	            result = ((String) o1).compareTo(o2.toString());
        	            break;

        	        case "java.lang.Double":
        	            if (o2 instanceof String) {
        	                o2 = Double.parseDouble((String) o2);
        	            }
        	            result = Double.compare((double) o1, (double) o2);
        	            break;

        	        case "java.util.Date":
        	            result = ((Date) o1).compareTo(convertObject(o2, Date.class));
        	            break;

        	        default:
        	            result = 0;
        	            break;
        	    }

        	    return result;
	}
       
        public static Object calculateMidpoint(Object minX, Object maxX) {
        if (minX instanceof Integer && maxX instanceof Integer) {
            return (int) ((int) minX + (int) maxX) / 2;
        } else if (minX instanceof Double && maxX instanceof Double) {
            return (double) ((double) minX + (double) maxX) / 2;
        } else if (minX instanceof String && maxX instanceof String) {
            String minString = (String) minX;
            String maxString = (String) maxX;
            int minLength = minString.length();
            int maxLength = maxString.length();
            int commonLength = Math.min(minLength, maxLength);
            int diffIndex = -1;
            
            for (int i = 0; i < commonLength; i++) {
                if (minString.charAt(i) != maxString.charAt(i)) {
                    diffIndex = i;
                    break;
                }
            }
            
            if (diffIndex == -1) {
                if (minLength < maxLength) {
                    return minString + maxString.substring(minLength);
                } else {
                    return maxString + minString.substring(maxLength);
                }
            } else {
                char minChar = minString.charAt(diffIndex);
                char maxChar = maxString.charAt(diffIndex);
                int midChar = (minChar + maxChar) / 2;
                return minString.substring(0, diffIndex) + (char) midChar;
            }
        } else if (minX instanceof Date && maxX instanceof Date) {
            long midpoint = ((Date) minX).getTime() + (((Date) maxX).getTime() - ((Date) minX).getTime()) / 2;
            return new Date(midpoint);
        } else {
            // throw an exception for unsupported data types
            throw new IllegalArgumentException("Unsupported data types: " + minX.getClass().getName() + ", " + maxX.getClass().getName());
        }
    }


    }

	public String getOctreePath() {
		return OctreePath;
	}

	public void setOctreePath(String octreePath) {
		OctreePath = octreePath;
	}}

