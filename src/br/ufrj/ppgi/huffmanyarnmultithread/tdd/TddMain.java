package br.ufrj.ppgi.huffmanyarnmultithread.tdd;

import java.io.IOException;

public class TddMain {

	public static void main(String[] args) throws IOException {
		if(SerializationUtilityTests.serializeAndDeserializeFrequencyArrayTest()) {
			System.out.println("Testes ok!");
			return;
		}
		
		System.out.println("Erro nos testes!");
	}
}
