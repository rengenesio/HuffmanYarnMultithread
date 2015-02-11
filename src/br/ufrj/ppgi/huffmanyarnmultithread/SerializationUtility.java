package br.ufrj.ppgi.huffmanyarnmultithread;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import br.ufrj.ppgi.huffmanyarnmultithread.encoder.Codification;


public class SerializationUtility {

	public static byte[] serializeCodificationArray(Codification[] codificationArray) {
		short lengthInBytes = 0;
		byte[] byteArray;
		for(short i = 0 ; i < codificationArray.length ; i++) {
			lengthInBytes += codificationArray[i].lengthInBytes;
		}
		
		byteArray = new byte[lengthInBytes];
		
		short i = 0;
		short index = 0;
		while(i < codificationArray.length) {
			System.arraycopy(codificationArray[i].toByteArray(), 0, byteArray, index, codificationArray[i].lengthInBytes);
			
			index += codificationArray[i].lengthInBytes;
			i++;
		}
		
		return byteArray;
	}
	
	
	public static Codification[] deserializeCodificationArray(byte[] byteArray) {
		ArrayList<Codification> codificationCollection = new ArrayList<Codification>();

		short i = 0;
		while(i < byteArray.length) {
			Codification codification = new Codification();
			codification.symbol = byteArray[i];
			codification.size = byteArray[i+1];
			codification.code = new byte[codification.size];
			System.arraycopy(byteArray, i+2, codification.code, 0, codification.size);

			codificationCollection.add(codification);
			
			i += (byteArray[i+1] + 2);
		}
		
		Codification[] codificationArray = new Codification[codificationCollection.size()];
		codificationCollection.toArray(codificationArray);
		return codificationArray;
	}
	
	public static byte[] serializeFrequencyArray(int[] frequencyArray) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
		
		for (int i = 0; i < frequencyArray.length; ++i) {
			dataOutputStream.writeInt(frequencyArray[i]);
		}

		return byteArrayOutputStream.toByteArray();
	}
	
	public static int[] deserializeFrequencyArray(byte[] byteArray) {
		int[] frequencyArray = new int[256];
		
		//System.arraycopy(byteArray, 0, frequencyArray, 0, byteArray.length);
		
		int index = 0;
		for(int i = 0 ; i < byteArray.length ; i += 4) {
			frequencyArray[index] += (byteArray[i] & 0xFF);
			frequencyArray[index] <<= 8;
			frequencyArray[index] += (byteArray[i+1] & 0xFF);
			frequencyArray[index] <<= 8;
			frequencyArray[index] += (byteArray[i+2] & 0xFF);
			frequencyArray[index] <<= 8;
			frequencyArray[index] += (byteArray[i+3] & 0xFF);

			index++;
		}
		
		
		return frequencyArray;
	}
}
