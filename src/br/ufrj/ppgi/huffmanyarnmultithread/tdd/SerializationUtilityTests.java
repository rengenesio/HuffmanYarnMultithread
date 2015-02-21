package br.ufrj.ppgi.huffmanyarnmultithread.tdd;

import java.io.IOException;

import br.ufrj.ppgi.huffmanyarnmultithread.Defines;
import br.ufrj.ppgi.huffmanyarnmultithread.SerializationUtility;

public class SerializationUtilityTests {

	public static boolean serializeAndDeserializeFrequencyArrayTest() throws IOException {
		System.out.println("Testando serialização e deserialização do array de frequências");
		
		
		long[] frequencyArray = new long[Defines.twoPowerBitsCodification];
		for(int i = 0 ; i < Defines.twoPowerBitsCodification ; i++) {
			long longRandom = new Double(Math.random() * 10000).longValue();
			frequencyArray[i] = longRandom;
		}
		
		byte[] serializedFrequencyArray = SerializationUtility.serializeFrequencyArray(frequencyArray);
		long[] deserializedFrequencyArray = SerializationUtility.deserializeFrequencyArray(serializedFrequencyArray);
		
		for(int i = 0 ; i < Defines.twoPowerBitsCodification ; i++) {
			if(frequencyArray[i] != deserializedFrequencyArray[i]) {
				return false;
			}
		}
		
		return true;
	}

}
