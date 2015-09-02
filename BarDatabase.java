import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 *   HBase data access class for Stock bar data.
 *     -Defines column names for daily volume, open, high, low, 
 *      and closing prices
 *     -Defines the row key structure
 *     -Methods to create schema, import data from CSV and query 
 */
public class BarDatabase  {

    // Schema Constants
    public final static byte[] TABLE_NAME = "BarData".getBytes();
    public final static byte[] COLUMN_FAMILY = "d".getBytes();	
    public final static byte[] COL_OPEN = "open".getBytes();
    public final static byte[] COL_HIGH = "high".getBytes();
    public final static byte[] COL_LOW = "low".getBytes();
    public final static byte[] COL_CLOSE = "close".getBytes();
    public final static byte[] COL_VOLUME = "volume".getBytes();

    // HBase configuration 
    private final Configuration config;

    // Creates a rowkey from date and stock name
    private byte[] makeKey(String date, String stock) {
        // Optimized so that stocks are close to each other lexicographically
	return (stock + date).getBytes();
    }

    public DataCallback GetDataImporter(String symbol) {
	return new LineImporter(symbol);
    }
    
    public String[] GetRow(String date, String symbol) throws IOException {
       	try (Connection conn = ConnectionFactory.createConnection(config)){
       	     Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
	       	byte[] key = makeKey(date, symbol);
		Get get = new Get(key);
	       	Result r = table.get(get);
	       	if ( r.isEmpty() ) return null;
	       	String[] retval = new String[6];
	       	retval[0] = new String(r.getRow());  // For validation
	       	retval[1] = new String(r.getValue(COLUMN_FAMILY, COL_OPEN));
	       	retval[2] = new String(r.getValue(COLUMN_FAMILY, COL_HIGH));
	       	retval[3] = new String(r.getValue(COLUMN_FAMILY, COL_LOW));
	       	retval[4] = new String(r.getValue(COLUMN_FAMILY, COL_CLOSE));
	       	retval[5] = new String(r.getValue(COLUMN_FAMILY, COL_VOLUME));
	       	return retval;
	    }
    }

    public String GetCell(String date, String symbol, byte[] column)
	throws IOException {
	try (Connection conn = ConnectionFactory.createConnection(config)){
	     Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
       		Get get = new Get(makeKey(date, symbol));
       		Result r = table.get(get);
       		if ( r.isEmpty() ) return null;
       		return new String(r.getValue(COLUMN_FAMILY, column));
       	}
    }

    public void ScanRows(String startDate, String symbol, 
			 int limit, DataScanner scanner) throws IOException {
        ResultScanner results = null;
	try (Connection conn = ConnectionFactory.createConnection(config)){
	     Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
		 Scan scan = new Scan();
		 scan.setStartRow(makeKey(startDate, symbol));
	       	 scan.setCaching(limit);
                 // Server side filter
		 scan.setFilter(new PageFilter(limit));  
	       	 results = table.getScanner(scan);
		 int count = 0;
	       	 for ( Result r : results ) {
       		     scanner.ProcessRow(r);
       		     if ( count++ >= limit ) break;
		 }
	}
	finally {
       	    if ( results != null ) results.close();			
	}

    }

    public void CreateTable() throws IOException {
	try (Connection connection = ConnectionFactory.createConnection(config);
	     Admin admin = connection.getAdmin()) {

       	    HTableDescriptor table =
		new HTableDescriptor(TableName.valueOf(TABLE_NAME));
	    table.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

       	    if (!admin.tableExists(table.getTableName())) {
      		System.out.print("Creating table. ");
   	     	admin.createTable(table);
       	       	System.out.println(" Done.");
	    }
	}
    }
	
    public void DropTable() throws IOException {
	try (Connection connection = ConnectionFactory.createConnection(config);
	     Admin admin = connection.getAdmin()) {

       	    HTableDescriptor table =
		new HTableDescriptor(TableName.valueOf(TABLE_NAME));
	    table.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

       	    if (admin.tableExists(table.getTableName())) {
      		System.out.print("Dropping table. ");
   	     	admin.disableTable(table.getTableName());
   	     	admin.deleteTable(table.getTableName());
       	       	System.out.println(" Done.");
	    }
	}
    }
	
    public BarDatabase(Configuration inConfig) {
       	config = inConfig;
    }

    // Internal class to do the work of processing a CSV line
    // into an HBase Put object.
    private class LineImporter implements DataCallback {

	// Set to true to skip a header line if present
       	private boolean skipFirst;
       	private final String currentStock;
       	private final List<Put> currentImport;

       	public LineImporter(String inSymbol, boolean inSkipFirst)  {
	    skipFirst = inSkipFirst;
	    currentStock = inSymbol;
	    currentImport = new ArrayList<Put>();
       	}

       	public LineImporter(String inSymbol) {
       	    this(inSymbol, true);  // Default assumes header
       	}

       	// Processes a CSV line from the file
       	// currentImport 
       	@Override
       	public void ProcessData(String line) throws IOException {
       	    if ( line == null ) return;
	    if ( skipFirst ) {
      		skipFirst = false;
      		return;
       	    }
       	    String[] data = line.split(","); 
       	    if ( data.length != 6 ) return;

       	    Put p = new Put(makeKey(data[0], currentStock));
       	    p.addColumn(COLUMN_FAMILY, COL_OPEN, data[1].getBytes());
       	    p.addColumn(COLUMN_FAMILY, COL_HIGH, data[2].getBytes());
       	    p.addColumn(COLUMN_FAMILY, COL_LOW, data[3].getBytes());
       	    p.addColumn(COLUMN_FAMILY, COL_CLOSE, data[4].getBytes());
       	    p.addColumn(COLUMN_FAMILY, COL_VOLUME, data[5].getBytes());
       	    currentImport.add(p);
       	}

	@Override
       	public void close() throws Exception {
       	    if ( currentImport.isEmpty() ) return;
            try (Connection conn = ConnectionFactory.createConnection(config)) {
		 Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
		      table.put(currentImport);
	       	      table.close();
	    }
	}

    }

    public static class ScanPrinter implements DataScanner {

	@Override
	public void close() throws Exception {}

       	@Override
       	public void ProcessRow(Result r) {
       	    System.out.print(new String(r.getRow()) + ",");
       	    System.out.print(new String(r.getValue(COLUMN_FAMILY, COL_OPEN)) + ",");
	    System.out.print(new String(r.getValue(COLUMN_FAMILY, COL_HIGH)) + ",");
       	    System.out.print(new String(r.getValue(COLUMN_FAMILY, COL_LOW)) + ",");
       	    System.out.print(new String(r.getValue(COLUMN_FAMILY, COL_CLOSE)) + ",");
       	    System.out.println(new String(r.getValue(COLUMN_FAMILY, COL_VOLUME)));
       	}
		
    }

}
