import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

/**
 *   Reads through a text file line by line and invokes a callback on each line.
 */
public class LineReader implements DataReader {

    private final String filename;
    private DataCallback callback;
	
    public LineReader(String inFilename, DataCallback inCallback) {
        filename = inFilename;
	callback = inCallback;
    }
    
    @Override
    public void ProcessData() throws FileNotFoundException, IOException {

	if ( filename == null || callback == null ) return;
      	File f = new File(filename);
	if ( !f.exists() || f.isDirectory() ) return;
		
	try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
            String line;					
	    while ( (line = reader.readLine()) != null ) {
    	        callback.ProcessData(line);
	    }			
	}
    }

    @Override
    public void setRecordCallback(DataCallback cback) {
	callback = cback;
    }

    @Override
    public void close() throws Exception {
        if ( callback != null ) callback.close();
    }
	
}
