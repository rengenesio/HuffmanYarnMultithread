package br.ufrj.ppgi.huffmanyarnmultithread.encoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithread.Defines;
import br.ufrj.ppgi.huffmanyarnmultithread.InputSplit;
import br.ufrj.ppgi.huffmanyarnmultithread.SerializationUtility;


public final class Encoder {
	
	// ------------- MASTER AND SLAVE CONTAINER PROPERTIES ------------- //
	
	// (YARN) YARN Configuration
	private Configuration configuration;
	
	// (YARN) Indicates if this container is the master container
	private boolean containerIsMaster = false;
	
	// (YARN) Total executing containers 
	private int numTotalContainers;
		
	// File to be processed
	private String fileName;
	
	// Collection with this container's input splits to be processed
	private ArrayList<InputSplit> inputSplitCollection;
	
	// Total input splits for container
	private int numTotalInputSplits = 0;
	
	// Array of byte arrays (each byte array represents a input split byte sequence) 
	private byte[][] memory;
	
	// Container total frequency array
	private long[] containerTotalFrequencyArray;
	
	// Associates a inputSplit with a index in memory matrix
	private Map<Integer, Integer> memoryPartMap;
	
	// Matrix to store each thread's frequency array
	private long[][] frequencyMatrix;
	
	// Número de símbolos que o container encontrou
	private short symbols = 0;
	
	// Array with Huffman codes
	private Codification[] codificationArray;
	
	// Max of threads in container
	private int maxThreads = 8;
	
	// Total threads to be spawn
	private int numTotalThreads = 1;
	
	// Queue to store sequencial id's (starts at 0) to threads
	private Queue<Integer> orderedThreadIdQueue;
	
	// Queue to store disk input splits indicator
	private Queue<InputSplit> diskQueue;
	
	// Queue to store memory input splits indicator
	private Queue<InputSplit> memoryQueue;
	
	
	// ------------------ MASTER CONTAINER PROPERTIES ------------------ //
	
	// (YARN) Master stores slaves containers listening ports
	private HostPortPair[] containerPortPairArray;
	
	// Total containers frequency array sum
	private long[] totalFrequencyArray;
	
	// Huffman's node array
	private NodeArray nodeArray;
	
	
	// ------------------ SLAVE CONTAINER PROPERTIES ------------------- //
	
	// (YARN) Master container hostname
	private String masterContainerHostName;
	
	// Port where slave container will listen for master connection
	private int slavePort;
	

	
	public Encoder(String[] args) {
		// Instantiates a YARN configuration
		this.configuration = new Configuration();

		// Reads filename from command line args
		this.fileName = args[0];

		// Instantiates a collection to store input splits metadata
		this.inputSplitCollection = new ArrayList<InputSplit>();
		
		// Splits command line arg in strings, each one represents an input split
		String[] inputSplitStringCollection = StringUtils.split(args[1], ':');
		
		// Iterates each string that represents an input split
		for(String inputSplitString : inputSplitStringCollection) {
			// Split an input split string in 3 fields (part, offset and length)
			String[] inputSplitFieldsCollection = StringUtils.split(inputSplitString, '-');
			
			// Instantiates a new input split
			InputSplit inputSplit = new InputSplit(Integer.parseInt(inputSplitFieldsCollection[0]), Integer.parseInt(inputSplitFieldsCollection[1]), Integer.parseInt(inputSplitFieldsCollection[2]));
			
			// Add this input split to input split collection
			this.inputSplitCollection.add(inputSplit);

			// The master container will be the one with the part 0
			if(inputSplit.part == 0) {
				this.containerIsMaster = true;
			}
		}

//
		for(InputSplit inputSplit : this.inputSplitCollection) {
			System.err.println("InputSplit: " + inputSplit.toString());
		}

		// Sets number of total input splits for this container
		this.numTotalInputSplits = this.inputSplitCollection.size();
		
		// Initializes a memory map with the max size (number of total splits) 
		this.memoryPartMap = new HashMap<Integer, Integer>(numTotalInputSplits);
		
		// Reads the master container hostname from command line args
		this.masterContainerHostName = args[2];
		
		// Reads the number of total containers from command line args
		this.numTotalContainers = Integer.parseInt(args[3]);
		
		// Initializes the queues with  
		this.diskQueue = new ArrayBlockingQueue<InputSplit>(this.numTotalInputSplits);
		this.memoryQueue = new ArrayBlockingQueue<InputSplit>(this.numTotalInputSplits);
	}
	
	
	public void encode() throws IOException, InterruptedException {
		chunksToMemory();
		
//
		for(Map.Entry<Integer, Integer> keyValuePair : this.memoryPartMap.entrySet()) {
			System.err.println("Chunk: " + keyValuePair.getKey() + "   Memory: " + keyValuePair.getValue());
		}

//
		if(this.containerIsMaster) {
			System.err.println("Host is master!!");
		}
		else {
			System.err.println("Host is not master!!");
		}
		
		
		// Número ideal de threads (1 para disco (se tiver alguma parte em disco) + X para memória, onde X é o número de chunks na memória)
		int idealNumThreads = this.memoryQueue.size() + (this.diskQueue.isEmpty() ? 0 : 1);

		if(idealNumThreads > this.maxThreads) {
			// Se o número ideal de threads for maior que o número máximo de threads, limito pelo número máximo de threads 
			this.numTotalThreads = this.maxThreads;
		}
		else {
			// Se o número ideal de threads for menor que o número máximo de threads, limito pelo número ideal
			this.numTotalThreads = idealNumThreads;
		}
		
		// Aloca o espaço onde cada thread vai contar os seus símbolos
		frequencyMatrix = new long[this.numTotalThreads][Defines.twoPowerBitsCodification];
		
		System.err.println("Número de threads a ser disparadas: " + this.numTotalThreads);
		

//
		System.err.println("Disco:");
		for(InputSplit inputSplit : diskQueue) {
			System.err.println(inputSplit);
		}
//		
		System.err.println("Memória:");
		for(InputSplit inputSplit : memoryQueue) {
			System.err.println(inputSplit);
		}
	
		ArrayList<Thread> threadCollection = new ArrayList<Thread>();
		for(int i = 0 ; i < numTotalThreads ; i++) {
			Thread thread = new Thread(new Runnable() {
				
				int threadId;
				
				@Override
				public void run() {
					// Semáforo de exclusão mútua para que duas threads não acessem ao mesmo tempo a fila que vai dar id's para as threads
					Semaphore threadIdQueueSemaphore = new Semaphore(1);
										
					// Thread usa o semáforo para acessar a fila para saber seu id
					try {
						threadIdQueueSemaphore.acquire();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					// Pega o próximo id da fila
					this.threadId = orderedThreadIdQueue.poll();

					// Fim da região de exclusão mútua
					threadIdQueueSemaphore.release();
					
					// Indica se esta thread é a responsável por fazer a contagem das partes em disco
					boolean diskThread = false;
					
					// A thread será responsável por fazer a contagem das partes do arquivo que estão em disco se ela for a primeira e tiver alguma parte em disco
					if(this.threadId == 0 && diskQueue.isEmpty() == false) {
						diskThread = true;
					}
					
					// Semáforo de exclusão mútua para que duas threads não acessem ao mesmo tempo a fila de partes do arquivo					
					Semaphore actionQueueSemaphore = new Semaphore(1);
					
					while(true) {
						InputSplit inputSplitToProcess = null;
						
						if(diskThread == false) {
							// Thread que entrar aqui vai pegar as partes da memória
							
							// Thread usa o semáforo para acessar a fila
							try {
								actionQueueSemaphore.acquire();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}

							// Pega o próximo elemento da fila (retorna nulo caso a fila esteja vazia)
							inputSplitToProcess = memoryQueue.poll();

							// Fim da região de exclusão mútua
							actionQueueSemaphore.release();
							
							// Encerra se a fila estiver vazia
							if(inputSplitToProcess == null) { return; }
						}
						else {
							// Thread que entrar aqui vai pegar as partes do disco (não precisa de exclusão mútua pois apenas uma thread é responsável pelos chunks que não foram carregados em memória)
							
							// Pega o próximo elemento da fila (retorna nulo caso a fila esteja vazia)
							inputSplitToProcess = diskQueue.poll();
							
							// Encerra se a fila estiver vazia
							if(inputSplitToProcess == null) { return; }
						}

//
						System.err.println("Thread " + this.threadId + "   inputSplit: " + inputSplitToProcess.toString());
						
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						
						try {
							chunkToFrequency(inputSplitToProcess);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				
				public void chunkToFrequency(InputSplit inputSplit) throws IOException {
					Integer memoryIndex = memoryPartMap.get(inputSplit.part);
					
					if(memoryIndex == null) {
						// Esta parte não está na memória, está no disco
//						
						System.err.println("Thread " + this.threadId + "   inputSplit: " + inputSplit.toString() + " (meu chunk está no disco)");
						
						FileSystem fs = FileSystem.get(configuration);
						Path path = new Path(fileName);
						
						FSDataInputStream f = fs.open(path);
						
						byte[] buffer = new byte[4096];
						
						int readBytes = -1;
						int totalReadBytes = 0;
						while(totalReadBytes < inputSplit.length) {
							readBytes = f.read(inputSplit.offset + totalReadBytes, buffer, 0, (totalReadBytes + 4096 > inputSplit.length ? inputSplit.length - totalReadBytes : 4096));
							for(int j = 0 ; j < readBytes ; j++) {
								frequencyMatrix[this.threadId][buffer[j] & 0xFF]++;
							}
							
							totalReadBytes += readBytes;
						}

//						
						System.err.println("Final TotalReadBytes: " + totalReadBytes);
					}
					else {
						// Esta parte está na memória
//						
						System.err.println("Thread " + this.threadId + "   inputSplit: " + inputSplit.toString() + " (meu chunk está no disco)");
						
						for (int j = 0; j < inputSplit.length ; j++) {
							frequencyMatrix[this.threadId][(memory[memoryIndex][j] & 0xFF)]++;
						}
					}
				}
			});
			
			threadCollection.add(thread);
			thread.start();
		}
		
		// Bloqueia até que todas as threads tenham terminado a contagem dos caracteres
		for(Thread thread : threadCollection) {
			thread.join();
		}
		
		this.containerTotalFrequencyArray = new long[256];		
		for(int i = 0 ; i < numTotalThreads ; i++) {
			for(int j = 0 ; j < 256 ; j++) {
				this.containerTotalFrequencyArray[j] += frequencyMatrix[i][j]; 
			}
		}
		
//
		int containerTotalSymbols = 0;
		for(int i = 0 ; i < 256 ; i++) {			
			System.err.println(i + " -> " + this.containerTotalFrequencyArray[i]);
			containerTotalSymbols += this.containerTotalFrequencyArray[i];
		}
//		
		System.out.println("CONTAINER TOTAL SYMBOLS: " + containerTotalSymbols);
	
//		// Matrix to store each slave serialized frequency (only master instantiates)
//		byte[][] serializedSlaveFrequency = null;
//
//		if(this.containerIsMaster) { // Master task (receive frequency data from all slaves)
//			// Instantiates matrix
//			serializedSlaveFrequency = new byte[numTotalContainers - 1][2048];
//			
//			// Stores informations about slaves, to connect to them to send codification data
//			this.containerPortPairArray = new HostPortPair[numTotalContainers - 1];
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
//			    containerPortPairArray[i] = new HostPortPair(clientSocket.getInetAddress().getHostName(), 3020 + i);
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
//					socket = new Socket(this.masterContainerHostName, 9996);
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
//			byte[] serializedFrequencyArray = SerializationUtility.serializeFrequencyArray(this.containerTotalFrequencyArray);
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
//		// Sequential part (only master container)
//		if(this.containerIsMaster) {
//			// Sums slaves frequency data
//			for(int i = 0 ; i < numTotalContainers - 1 ; i++) {
//				// Deserialize client frequency data
//				long[] slaveFrequencyArray = SerializationUtility.deserializeFrequencyArray(serializedSlaveFrequency[i]);
//	
//				// Sums
//				for(short j = 0 ; j < 256 ; j++) {
//					totalFrequencyArray[j] += slaveFrequencyArray[j];
//				}
//			}
//			
//			// Free slaves received data
//			serializedSlaveFrequency = null;
//			
//			// Add EOF
//		    totalFrequencyArray[0] = 1;
//		    
//		    // Count total symbols
//		    this.symbols = 0;
//		    long totalSymbols = 0;
//		    for(short i = 0 ; i < 256 ; i++) {
//		    	if(this.totalFrequencyArray[i] != 0) {
//		    		this.symbols++;
//		    		totalSymbols += this.totalFrequencyArray[i];
//		    	}
//		    }
//		    
//		    System.err.println("TOTAL FREQUENCY:");
//		    for(short i = 0 ; i < 256 ; i++) {
//		    	System.err.println(i + " ->  " + this.totalFrequencyArray[i]);
//		    }
//		    System.err.println("Total symbols: " + totalSymbols);
//		    
////		    this.frequencyToNodeArray();
////			this.huffmanEncode();
////			this.treeToCode();
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
		FileSystem fs = FileSystem.get(this.configuration);
		Path path = new Path(fileName);
		
		FSDataInputStream f = fs.open(path);
		
		memory = new byte[this.numTotalInputSplits][];

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
					memoryQueue.add(inputSplitCollection.get(i));
				}
				catch(Error error) {
					// Adiciona este chunk na lista de ações a serem feitas do disco
					diskQueue.add(inputSplitCollection.get(i));
					
					// Seta a variável de controle para que ele não tente alocar mais espaço na memória
					memoryFull = true;
				}
			}
			else {
				// Adiciona este chunk na lista de ações a serem feitas do disco
				diskQueue.add(inputSplitCollection.get(i));
			}
			
			i++;
		}
	}
	
	
		
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
