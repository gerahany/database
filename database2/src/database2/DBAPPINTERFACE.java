package database2;

import java.util.Hashtable;
import java.util.Iterator;

public interface DBAPPINTERFACE {

	public void init( );
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax )
			throws DBAppException;
	
	
	public void insertIntoTable(String strTableName,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException;
	
	
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue )
			throws DBAppException;
	
	public void deleteFromTable(String strTableName,
			Hashtable<String,Object> htblColNameValue)
			throws DBAppException;
	void createIndex(String tableName, String[] columnNames) throws DBAppException;

    Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws 
    DBAppException;
	
	
}
