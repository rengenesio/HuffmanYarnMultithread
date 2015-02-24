package br.ufrj.ppgi.huffmanyarnmultithread;


public class Defines {
	public static final String jobName = "HuffmanYarnMultithread"; 
	
	public static final int amMemory = 10;
	public static final int amVCores = 1;
	public static final int amPriority = 0;
	public static final String amQueue = "default";
	
	
	public static final int containerMemory = 8192;
	//public static final int containerMemory = 512;
	public static final int containerVCores = 8;
	
	
	
	
	// Huffman constants
	public static final int bitsCodification = 8;
	public static final int twoPowerBitsCodification = 256;
	
	
	public static final int readBufferSize = 8192;
	public static final int maxChunksInMemory = 16;
	
	
	
	
	public static final String pathSuffix = ".yarnmultithreaddir/";
	public static final String compressedPath = "compressed/";
	public static final String compressedFileName = "part-";
	public static final String codificationFileName = "codification";
	public static final String decompressedFileName = "decompressed";
}
