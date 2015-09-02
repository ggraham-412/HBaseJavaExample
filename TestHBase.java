
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestHBase {

    private static void ImportCSVData(BarDatabase db,
				      String filename, String symbol) {
	try (LineReader reader =
	        new LineReader(filename, db.GetDataImporter(symbol))) {
  	    reader.ProcessData();
	}
	catch(Exception e) {
	    e.printStackTrace();
	}	
    }
    
    public static void main(String... args) throws IOException {

       	Configuration config = HBaseConfiguration.create();
       	config.addResource(new Path(System.getenv("HBASE_CONF_DIR"),
				    "hbase-site.xml"));
       	BarDatabase db = new BarDatabase(config);

	db.DropTable();

	db.CreateTable();

        ImportCSVData(db, "FinData/GOOG-NYSE_ABT.csv", "ABT");
        ImportCSVData(db, "FinData/GOOG-NYSE_BMY.csv", "BMY");
        ImportCSVData(db, "FinData/GOOG-NYSE_MRK.csv", "MRK");
        ImportCSVData(db, "FinData/GOOG-NYSE_PFE.csv", "PFE");

       	String[] sResult = db.GetRow("2015-08-28", "BMY");
       	System.out.print(sResult[0] + ",");
       	System.out.print(sResult[1] + ",");
       	System.out.print(sResult[2] + ",");
       	System.out.print(sResult[3] + ",");
       	System.out.print(sResult[4] + ",");
       	System.out.println(sResult[5]);
	
        System.out.println("BMY 8/28 close is " +
	     db.GetCell("2015-08-28", "BMY", BarDatabase.COL_CLOSE));
        
        db.ScanRows("2015-08-17", "BMY", 10, new BarDatabase.ScanPrinter());

    }
}
