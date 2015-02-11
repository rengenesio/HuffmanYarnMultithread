package br.ufrj.ppgi.huffmanyarnmultithread.decoder;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithread.SerializationUtility;
import br.ufrj.ppgi.huffmanyarnmultithread.encoder.BitSet;
import br.ufrj.ppgi.huffmanyarnmultithread.encoder.Codification;


public class Decoder {
	Codification[] codificationArray;
	short symbols;
	byte max_code = 0;
	Path in, out, cb;
	byte[] codificationArrayElementSymbol;
	boolean[] codificationArrayElementUsed;

	public Decoder(String file_in, String file_out, String file_cb)
			throws IOException {
		in = new Path(file_in);
		out = new Path(file_out);
		cb = new Path(file_cb);

		fileToCodification();
		codeToTreeArray();
		huffmanDecode();
	}

	public void fileToCodification() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream f = fs.open(cb);

		byte[] byteArray = new byte[f.available()];
		f.readFully(byteArray);

		this.codificationArray = SerializationUtility.deserializeCodificationArray(byteArray);
		
		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codificationArray[i].toString());
		*/
	}

	public void codeToTreeArray() {
		for(short i = 0 ; i < this.codificationArray.length ; i++) {
			this.max_code = (this.codificationArray[i].size > this.max_code) ? this.codificationArray[i].size : this.max_code;  
		}
		
		codificationArrayElementSymbol = new byte[(int) Math.pow(2, (max_code + 1))];
		codificationArrayElementUsed = new boolean[(int) Math.pow(2, (max_code + 1))];

		for (short i = 0; i < this.codificationArray.length; i++) {
			int index = 0;
			for (byte b : codificationArray[i].code) {
				index <<= 1;
				if (b == 0)
					index += 1;
				else
					index += 2;
			}
			codificationArrayElementSymbol[index] = codificationArray[i].symbol;
			codificationArrayElementUsed[index] = true;
		}

		/*
		System.out.println("codeToTreeArray():");
		System.out.println("TREE_ARRAY:"); 
		for(int i = 0 ; i < Math.pow(2,(max_code + 1)) ; i++) 
			if(codificationArrayElementUsed[i])
				System.out.println("i: " + i + " -> " + codificationArrayElementSymbol[i]);
		System.out.println("------------------------------");
		*/
	}
	
	public void huffmanDecode() throws IOException {
		byte[] buffer = new byte[1];
		BitSet bufferBits = new BitSet();
		
		int index = 0;

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(in);
		FSDataOutputStream fout = fs.create(out);

		for (short i = 0; i < status.length; i++) {
			FSDataInputStream fin = fs.open(status[i].getPath());

			while (fin.available() > 0) {
				fin.read(buffer, 0, 1);
				bufferBits.fromByte(buffer[0]);
				for (byte j = 0; j < 8 ; j++) {
					index <<= 1;
					if (bufferBits.cheackBit(j) == false)
						index += 1;
					else
						index += 2;

					if (codificationArrayElementUsed[index]) {
						if (codificationArrayElementSymbol[index] != 0) {
							fout.write(codificationArrayElementSymbol[index]);
							index = 0;
						} else {
							index = 0;
							break;
						}
					}
				}
			}
			fin.close();
		}
		fout.close();
	}
}