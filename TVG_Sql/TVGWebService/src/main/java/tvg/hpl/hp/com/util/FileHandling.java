/**
 * 
 */
package tvg.hpl.hp.com.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author sarmaji
 *
 */
public class FileHandling {
	FileWriter fileWriter;
	BufferedWriter bufferedWriter;
	public BufferedWriter WriteToFile(String filename){
		
		File file = new File(filename);
		if (!file.exists()){
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			fileWriter = new FileWriter(file.getAbsolutePath(),true);
			bufferedWriter = new BufferedWriter(fileWriter);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return bufferedWriter;
	}
}
