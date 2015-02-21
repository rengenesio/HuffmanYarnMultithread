package br.ufrj.ppgi.huffmanyarnmultithread.encoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithread.InputSplit;


public final class Encoder {
	// Configuration
	private Configuration conf;
	
	private String fileName;
	private ArrayList<InputSplit> inputSplitCollection;
	private String masterHostName;
	private int numTotalContainers;
	private int slavePort;
	private HostPortPair[] hostPortPairArray;
	
	// Array of byte arrays (each byte array represents a input split byte sequence) 
	private byte[][] memory;	
	
	// Associates a inputSplit with a index in memory matrix
	//private int[] memoryPartMap;
	private Map<Integer, Integer> memoryPartMap; 
	
	private int[][] frequencyMatrix;
	private long[] totalFrequencyArray;
	private short symbols = 0;
	private NodeArray nodeArray;
	private Codification[] codificationArray;
	
	private int numTotalInputSplits = 0;
	
	private int maxThreads = 8;
	private int numTotalThreads = 1;
	
	private LinkedBlockingQueue<Integer> diskActionQueue = new LinkedBlockingQueue<Integer>();
	private LinkedBlockingQueue<Integer> memoryActionQueue = new LinkedBlockingQueue<Integer>();
	
	public Encoder(String[] args) {
		this.conf = new Configuration();

		this.fileName = args[0];

		this.inputSplitCollection = new ArrayList<InputSplit>();
		
		String[] inputSplitStringCollection = StringUtils.split(args[1], ':');
		for(String inputSplitString : inputSplitStringCollection) {
			String[] inputSplitFieldsCollection = StringUtils.split(inputSplitString, '-');
			this.inputSplitCollection.add(new InputSplit(Integer.parseInt(inputSplitFieldsCollection[0]), Integer.parseInt(inputSplitFieldsCollection[1]), Integer.parseInt(inputSplitFieldsCollection[2])));
		}

		for(InputSplit inputSplit : this.inputSplitCollection) {
			System.out.println(inputSplit.toString());
		}
		
		this.numTotalInputSplits = this.inputSplitCollection.size();
		
		this.memoryPartMap = new HashMap<Integer, Integer>(numTotalInputSplits);
		
		this.masterHostName = args[2];
		this.numTotalContainers = Integer.parseInt(args[3]);
	}
	
	
	public void encode() throws IOException, InterruptedException {
		if(this.numTotalInputSplits >= this.maxThreads) {
			this.numTotalThreads = this.maxThreads;
		} else {
			this.numTotalThreads = this.numTotalInputSplits;
		}

		chunksToMemory();
		
		System.out.println("Memória:");
		while(memoryActionQueue.isEmpty() == false) {
			int chunk = memoryActionQueue.take();
			System.out.println(chunk);
		}
		
		System.out.println("\nDisco:");
		while(diskActionQueue.isEmpty() == false) {
			int chunk = diskActionQueue.take();
			System.out.println(chunk);
		}
		
//		ArrayList<Thread> threadCollection = new ArrayList<Thread>();
//		for(int i = 0 ; i < numTotalThreads ; i++) {
//			Thread thread = new Thread(new Runnable() {
//				
//				@Override
//				public void run() {
//					Semaphore actionQueueSemaphore = new Semaphore(1);
//					
//					while(true) {
//						try {
//							actionQueueSemaphore.acquire();
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}
//
//						String action = null;
//						do {
//							try {
//								action = actionQueue.take();
//							} catch (InterruptedException e) {
//								e.printStackTrace();
//							}
//							
//						}while(action)
//						
//						
//						
//						actionQueueSemaphore.release();
//
//						//Thread.currentThread().getId();
//						
//						
//					}
//				}
//			});
//			
//			threadCollection.add(thread);
//			thread.start();
//		}
//		
//		// Bloqueia até que todas as threads tenham terminado a contagem dos caracteres
//		for(Thread thread : threadCollection) {
//			thread.join();
//		}
	
//		
//		memoryToFrequency();
//
//		// Matrix do store each slave serialized frequency (only master instantiates)
//		byte[][] serializedSlaveFrequency = null;
//
//		// Communication between slaves and master
//		if(this.inputOffset == 0) { // Master task (receive frequency data from all slaves)
//			// Instantiates matrix
//			serializedSlaveFrequency = new byte[numTotalContainers - 1][1024];
//			
//			// Stores informations about slaves, to connect to them to send codification data
//			this.hostPortPairArray = new HostPortPair[numTotalContainers - 1];
//			
//			// Instantiates a socket that listen for connections
//			ServerSocket serverSocket = new ServerSocket(9996, numTotalContainers);
//			
//			for(int i = 0 ; i < numTotalContainers -1 ; i++) {
////				
//				System.out.println("Master aguardando client: " + i);
//				
//				// Blocked waiting for some slave connection
//				Socket clientSocket = serverSocket.accept();
////				
//				System.out.println("Client conectou!");
//				
//				// When slave connected, instantiates stream to receive slave's frequency data
//			    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
////
//			    System.out.println("Bytes que vou ler: " + 1024);
//			    
//			    // Reads serialized data from slave
//			    dataInputStream.readFully(serializedSlaveFrequency[i], 0, 1024);
////			    
//			    System.out.println("Li tudo!!");
//			    
//			    // Instantiates stream to send to slave a port where the slave will listen a connection to receive the codification data
//			    DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
//			    dataOutputStream.writeInt(3020 + i);
//			    
//			    // Stores information about this slave and the port it received
//			    hostPortPairArray[i] = new HostPortPair(clientSocket.getInetAddress().getHostName(), 3020 + i);
//			    
//			    // Close socket with slave
// 			    clientSocket.close();
////			    
//			    System.out.println("Master recebeu do client: " + i);
//			}
//			
//			// Close ServerSocket after receive from all slaves
//			serverSocket.close();
//		}
//		else { // Slave task (send frequency to master)
////			
//			System.out.println("Client tentando conectar com master");
//	
//			Socket socket;
//			// Blocked until connect to master (sleep between tries)
//			while(true) {
//				try {
//					socket = new Socket(this.masterHostName, 9996);
//					break;
//				} catch(Exception e) {
//					Thread.sleep(1000);
//				}
//			}
//			
//			// When connected to master, instantiates a stream to send frequency data
//			DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
//			
//			// Serialized frequency data
//			byte[] serializedFrequencyArray = SerializationUtility.serializeFrequencyArray(this.frequencyArray);
//			
//			// Sends serialized frequency data
//			dataOutputStream.write(serializedFrequencyArray, 0, serializedFrequencyArray.length);
//			
//			// Instantiates a stream to receive a port number
//			DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
//			this.slavePort = dataInputStream.readInt();
//			
////			
//			System.out.println("Client enviou para o master");
//			
//			// Close socket
//			socket.close();
//		}
//		
//		
//		// Sequential part (only master)
//		if(this.inputOffset == 0) {
//			// Sums slaves frequency data
//			for(int i = 0 ; i < numTotalContainers - 1 ; i++) {
//				// Deserialize client frequency data
//				int[] slaveFrequencyArray = SerializationUtility.deserializeFrequencyArray(serializedSlaveFrequency[i]);
//	
//				// Sums
//				for(short j = 0 ; j < 256 ; j++) {
//					totalFrequency[j] += slaveFrequencyArray[j];
//				}
//			}
//			
//			// Free slaves received data
//			serializedSlaveFrequency = null;
//			
//			// Add EOF
//		    totalFrequency[0] = 1;
//		    
//		    // Count total symbols
//		    this.symbols = 0;
//		    for(short i = 0 ; i < 256 ; i++) {
//		    	if(this.totalFrequency[i] != 0) {
//		    		this.symbols++;
//		    	}
//		    }
//		    
//		    this.frequencyToNodeArray();
//			this.huffmanEncode();
//			this.treeToCode();
//		}
//		
//		// Communication between slaves and master 
//		if(this.inputOffset == 0) { // Master task (send codification data to all slaves)
//			// Serializes codification data
//			byte[] serializedCodification = SerializationUtility.serializeCodificationArray(codificationArray);
////			
//			System.out.println("Serialized codification length: " + serializedCodification.length);
//			
//			// Send codification data to all slaves
//			for(int i = 0 ; i < numTotalContainers - 1 ; i++) {
////				
//				System.out.println("Master abrindo porta para aguardar client: " + i);
//				
//				Socket socket;
//				// Blocked until connect to slave (sleep between tries)
//				while(true) {
//					try {
//						socket = new Socket(hostPortPairArray[i].hostName, hostPortPairArray[i].port);
//						break;
//					} catch(Exception e) {
//						Thread.sleep(1000);
//					}
//				}
//				
//				// Instantiates stream to send data to slave
//				DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
//				
//				// Send size of serialized codification to slave
//				dataOutputStream.writeShort(serializedCodification.length);
//				
//				// Send serialized codification to slave
//				dataOutputStream.write(serializedCodification, 0, serializedCodification.length);
////				
//				System.out.println("Master recebeu do client: " + i);
//				
//				// Close socket with slave
//				socket.close();
//			}
//		
//			codificationToHDFS();
//		}
//		else { // Slaves task (receive codification data from master)
////			
//			System.out.println("Client abrindo porta para aguardar master : " + slavePort);
//			
//			// Instantiates the socket for receiving data from master
//			ServerSocket serverSocket = new ServerSocket(slavePort);
//			
//			// Blocked until master connection
//		    Socket clientSocket = serverSocket.accept();
//		    
//		    // When master connects, instantiates stream to receive data
//		    DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
//		    
//		    // Number of bytes that client will read
//		    short serializedCodificationLength = dataInputStream.readShort();
//
//		    // Array to store serialized codification received
//		    byte[] serializedCodification = new byte[serializedCodificationLength];
//		    
//		    // Receives serialized codification
//		    dataInputStream.readFully(serializedCodification, 0, serializedCodificationLength);
//		    
//		    // Close socket with master
//		    serverSocket.close();
//		    
////
//		    System.out.println("Client recebeu do master");
//		    
//		    // Deserializes codification received
//		    this.codificationArray = SerializationUtility.deserializeCodificationArray(serializedCodification);
//		}
//
//		// Master and slaves task
//		memoryCompressor();
	}

	private void chunksToMemory() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(fileName);
		
		FSDataInputStream f = fs.open(path);
		
		memory = new byte[this.numTotalInputSplits][];
		//memoryPartMap = new int[this.numTotalInputSplits];


		int i = 0;
		boolean memoryFull = false;
		while(i < this.inputSplitCollection.size()) {
			// Adiciona mais 1 no tamanho do split pra poder adicionar na memória o marcador de fim do bloco da compressão (EOF)
			inputSplitCollection.get(i).length++;
			
			if(memoryFull == false) {
				try {
					// Tenta alocar espaço na memória para este chunk. Se não conseguir, um erro será lançado e capturado
					memory[i] = new byte[(int) inputSplitCollection.get(i).length];
					
					// Lê o chunk do disco para o bloco de memória que acabou de ser alocado
					f.read(inputSplitCollection.get(i).offset, memory[i], 0, inputSplitCollection.get(i).length);
					
					// Adiciona o marcador de final de bloco na compressão
					memory[i][inputSplitCollection.get(i).length - 1] = 0;
					
					// Mapeia o número do chunk lido para um índice do array da memória
					memoryPartMap.put(inputSplitCollection.get(i).part, i);
					
					// Adiciona este chunk na lista de ações a serem feitas da memória
					memoryActionQueue.add(inputSplitCollection.get(i).part);
					
				}
				catch(Error error) {
					// Adiciona este chunk na lista de ações a serem feitas do disco
					diskActionQueue.add(inputSplitCollection.get(i).part);
					
					// Seta a variável de controle para que ele não tente alocar mais espaço na memória
					memoryFull = true;
				}
			}
			else {
				// Adiciona este chunk na lista de ações a serem feitas do disco
				diskActionQueue.add(inputSplitCollection.get(i).part);
			}
		}
	}
	
//	public void chunkToFrequency(int threadId, int chunk) throws IOException {
//		
//		
//		if(this.memoryPartMap.get(chunk) == null) {
//			// Esta parte não está na memória, está no disco
//		}
//		else {
//			// Esta parte está na memória
//			for (int i = 0; i < inputLength; i++) {
//				frequencyArray[(memory[i] & 0xFF)]++;
//			}
//		}
//		
//		if(this.inputOffset == 0) {
//			this.totalFrequency = new long[256];
//			for (int i = 0; i < inputLength; i++) {
//				totalFrequency[(memory[i] & 0xFF)]++;
//			}
//		}
//		else {
//			this.frequencyArray = new int[256];
//			for (int i = 0; i < inputLength; i++) {
//				frequencyArray[(memory[i] & 0xFF)]++;
//			}
//		}
//		
//        /*
//        System.out.println("FREQUENCY: symbol (frequency)");
//        for (int i = 0; i < frequency.length; i++)
//                if (frequency[i] != 0)
//                        System.out.println((int) i + "(" + frequency[i] + ")");
//        System.out.println("------------------------------");
//        */
//	}
//	
//	
//	public void frequencyToNodeArray() {
//		this.nodeArray = new NodeArray((short) this.symbols);
//
//		for (short i = 0 ; i < 256 ; i++) {
//			if (this.totalFrequency[i] > 0) {
//				this.nodeArray.insert(new Node((byte) i, this.totalFrequency[i]));
//				System.out.print(i + " ");
//			}
//		}
//
//		/*
//		System.out.println(nodeArray.toString());
//		*/
//	}
//
//	public void huffmanEncode() {
//		while (this.nodeArray.size() > 1) {
//			Node a, b, c;
//			a = this.nodeArray.get(this.nodeArray.size() - 2);
//			b = this.nodeArray.get(this.nodeArray.size() - 1);
//			c = new Node((byte) 0, a.frequency + b.frequency, a, b);
//
//			this.nodeArray.removeLastTwoNodes();
//			this.nodeArray.insert(c);
//			
//			/*
//			System.out.println(nodeArray.toString() + "\n");
//			*/
//		}
//	}
//	
//	public void treeToCode() {
//		Stack<Node> s = new Stack<Node>();
//		//codification = new Codification[symbols];
//		codificationArray = new Codification[symbols];
//		//codificationCollection = new CodificationArray();
//		Node n = nodeArray.get(0);
//		short codes = 0;
//		byte[] path = new byte[33];
//		
//		byte size = 0;
//		s.push(n);
//		while (codes < symbols) {
//			if (n.left != null) {
//				if (!n.left.visited) {
//					s.push(n);
//					n.visited = true;
//					n = n.left;
//					path[size++] = 0;
//				} else if (!n.right.visited) {
//					s.push(n);
//					n.visited = true;
//					n = n.right;
//					path[size++] = 1;
//				} else {
//					size--;
//					n = s.pop();
//				}
//			} else {
//				n.visited = true;
//				codificationArray[codes] = new Codification(n.symbol, size, path);
//				n = s.pop();
//				size--;
//				codes++;
//			}
//		}
//
//		/*
//		System.out.println(symbols);
//		System.out.println("CODIFICATION: symbol (size) code"); 
//		for (short i = 0; i < codificationArray.length ; i++)
//			System.out.println(codificationArray[i].toString());
//		*/
//	}
//	
//	public void codificationToHDFS() throws IOException {
//		FileSystem fs = FileSystem.get(this.conf);
//		Path path = new Path(fileName + ".dir/codification");
//		FSDataOutputStream f = fs.create(path);
//		
//		byte[] codificationSerialized = SerializationUtility.serializeCodificationArray(this.codificationArray);
//		f.write(codificationSerialized);
//		f.close();
//	}
//	
//    public void memoryCompressor() throws IOException {
//    	FileSystem fs = FileSystem.get(this.conf);
//		Path path = new Path(fileName + ".dir/compressed/part-" + String.format("%08d", this.inputPartId));
//		
//		FSDataOutputStream f = fs.create(path);
//		
//		BitSet buffer = new BitSet();
//
//        byte bits = 0;
//        for (int i = 0; i < memory.length ; i++) {
//            for (short j = 0; j < this.codificationArray.length ; j++) {
//                if (this.memory[i] == this.codificationArray[j].symbol) {
//                    for (byte k = 0; k < codificationArray[j].size; k++) {
//                        if (codificationArray[j].code[k] == 1)
//                                buffer.setBit(bits, true);
//                        else
//                                buffer.setBit(bits, false);
//
//                        if (++bits == 8) {
//                                f.write(buffer.b);
//                        		buffer = new BitSet();
//                                bits = 0;
//                        }
//                    }
//                    break;
//                }
//            }
//        }
//
//        if (bits != 0) {
//        	f.write(buffer.b);
//        }
//        
//
//        f.close();
//	}

	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Encoder encoder = new Encoder(args);
		encoder.encode();
	}
}
