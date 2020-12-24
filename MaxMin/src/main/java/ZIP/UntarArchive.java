package ZIP;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import java.util.Scanner;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class UntarArchive {
	public static void main(String[] args) throws Exception {
		int j = 0;
		for (int i = 1901; i < 1920; i++) {
			try (BufferedInputStream inputStream = new BufferedInputStream(
					new URL("https://www.ncei.noaa.gov/data/global-hourly/archive/csv/" + i + ".tar.gz").openStream());
					FileOutputStream fileOS = new FileOutputStream("/home/cloudera/inputs1/" + i + ".tar.gz")) {
				byte data[] = new byte[1024];
				int byteContent;
				while ((byteContent = inputStream.read(data, 0, 1024)) != -1) {
					fileOS.write(data, 0, byteContent);
				}
			} catch (IOException e) {
			}
			System.out.println("download of "+i+" success !");
			String COMPRESSED_FILE = "/home/cloudera/inputs1/" + i + ".tar.gz";
			String DESTINATION_PATH = "/home/cloudera/inputs2/";
			File destFile = new File(DESTINATION_PATH + i);
			unTarFile(COMPRESSED_FILE, destFile);
			j = j + 1;
		}

		File dir = new File("/home/cloudera/inputs2");
		File[] folder1 = dir.listFiles();

		for (File table : folder1) {
			File[] filenames = table.listFiles();
			for (File file : filenames) {
				BufferedReader csvReader = new BufferedReader(new FileReader(file.getPath()));
				String line = "";
				while ((line = csvReader.readLine()) != null) {
					String[] data = line.split(",");
					String csv = "/home/cloudera/inputs3/" + table.getName();
					line.replace(",", "+");

					CSVWriter writer = new CSVWriter(new FileWriter(csv, true));
					if(data[14].contains("DEW")==false) {
						writer.writeNext(data);
					}
					writer.close();
				}
				csvReader.close();
			}
		}

		for (int d = 1901; d < 1920; d++) {
			File file = new File("/home/cloudera/inputs3/" + d); 
			Scanner scanner = new Scanner(file); 
			PrintWriter writer = new PrintWriter("/home/cloudera/inputsData/" + d); 
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String newLine = "";
				for (int i = 0; i < line.length(); i++) {
					line.replace(",", "+");
					if (line.charAt(i) != '"') {
						newLine = newLine + line.charAt(i);
					} else if (line.compareTo("EW DATE") == 0) {

					}
				}
				writer.println(newLine);
			}
		}
	}

	private static void unTarFile(String tarFile, File destFile) {
		TarArchiveInputStream tis = null;
		try {
			FileInputStream fis = new FileInputStream(tarFile);
			// .gz
			GZIPInputStream gzipInputStream = new GZIPInputStream(new BufferedInputStream(fis));
			// .tar.gz
			tis = new TarArchiveInputStream(gzipInputStream);
			TarArchiveEntry tarEntry = null;
			while ((tarEntry = tis.getNextTarEntry()) != null) {
				System.out.println(" Untarring of- " + tarEntry.getName());
				if (tarEntry.isDirectory()) {
					continue;
				} else {
					File outputFile = new File(destFile + File.separator + tarEntry.getName());
					outputFile.getParentFile().mkdirs();
					IOUtils.copy(tis, new FileOutputStream(outputFile));
				}
			}
		} catch (IOException ex) {
			System.out.println("Error while untarring a file- " + ex.getMessage());
		} finally {
			if (tis != null) {
				try {
					tis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
