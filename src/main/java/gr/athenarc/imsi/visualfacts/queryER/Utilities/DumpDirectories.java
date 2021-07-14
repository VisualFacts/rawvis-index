package gr.athenarc.imsi.visualfacts.queryER.Utilities;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class DumpDirectories {
	/**
	 * 
	 */
	private static  String dataDirPath;
	private static String logsDirPath;
	private static String blockDirPath;
	private static String blockIndexDirPath;
	private static String groundTruthDirPath;
	private static String tableStatsDirPath;
	private static String blockIndexStatsDirPath;
	private static String linksDirPath;
	private static String similaritiesDirPath;
	private static String liFilePath;
	private static String qIdsPath;
	private static String vetiPath;

	public DumpDirectories() {
		super();
	}

//	public static DumpDirectories loadDirectories() {
//		if(new File(dataDirPath + "/dumpMap.json").exists()) {
//			ObjectMapper objectMapper = new ObjectMapper();  
//			try {
//				return objectMapper.readValue(new File(dataDirPath + "/dumpMap.json"),	DumpDirectories.class);
//
//			} catch (JsonParseException e) {
//				e.printStackTrace();
//			} catch (JsonMappingException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		return new DumpDirectories("");
//	}
	
	public DumpDirectories(String dumpPath){
		dataDirPath = dumpPath;
		logsDirPath = dumpPath + "/logs/";
		blockDirPath = dumpPath + "/blocks/";
		blockIndexDirPath = dumpPath + "/blockIndex/";
		groundTruthDirPath = dumpPath + "/groundTruth/";
		tableStatsDirPath = dumpPath + "/tableStats/tableStats/";
		blockIndexStatsDirPath = dumpPath + "/tableStats/blockIndexStats/";
		linksDirPath = dumpPath + "/links/";
		similaritiesDirPath = dumpPath + "/links/";
		qIdsPath = dumpPath + "/qIds/";
		vetiPath = dumpPath + "/veti/";
		liFilePath = dumpPath + "/LI";
	}

	public  void generateDumpDirectories() throws IOException {
		File dataDir = new File(dataDirPath);
		File logsDir = new File(logsDirPath);
		File blockDir = new File(blockDirPath);
		File blockIndexDir = new File(blockIndexDirPath);
		File groundTruthDir = new File(groundTruthDirPath);
		File tableStatsDir = new File(tableStatsDirPath);
		File blockIndexStats = new File(blockIndexStatsDirPath);
		File linksDir = new File(linksDirPath);
		File vetiDir = new File(vetiPath);
		File similaritiesDir = new File(similaritiesDirPath);
		if(!dataDir.exists()) {
			FileUtils.forceMkdir(dataDir); //create directory
		}
		if(!logsDir.exists()) {
			FileUtils.forceMkdir(logsDir); //create directory
		}
		if(!blockIndexDir.exists()) {
			FileUtils.forceMkdir(blockIndexDir); //create directory
		}
		if(!groundTruthDir.exists()) {
			FileUtils.forceMkdir(groundTruthDir); //create directory
		}
		if(!tableStatsDir.exists()) {
			FileUtils.forceMkdir(tableStatsDir); //create directory
		}
		if(!blockIndexStats.exists()) {
			FileUtils.forceMkdir(blockIndexStats); //create directory
		}
		if(!linksDir.exists()) {
			FileUtils.forceMkdir(linksDir); //create directory
		}
		if(!similaritiesDir.exists()) {
			FileUtils.forceMkdir(similaritiesDir); //create directory
		}
		if(!vetiDir.exists()) {
			FileUtils.forceMkdir(vetiDir); //create directory
		}

	}

	
	public String getDataDirPath() {
		return dataDirPath;
	}

	public String getLogsDirPath() {
		return logsDirPath;
	}

	public String getBlockDirPath() {
		return blockDirPath;
	}

	public String getBlockIndexDirPath() {
		return blockIndexDirPath;
	}

	public String getGroundTruthDirPath() {
		return groundTruthDirPath;
	}

	public String getTableStatsDirPath() {
		return tableStatsDirPath;
	}

	public String getBlockIndexStatsDirPath() {
		return blockIndexStatsDirPath;
	}

	public String getLinksDirPath() {
		return linksDirPath;
	}
	public String getqIdsPath() {
		return qIdsPath;
	}

	public String getVetiPath() {
		return vetiPath;
	}
	
	public String getLiFilePath() {
		return liFilePath;
	}
	
}
