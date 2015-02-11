package br.ufrj.ppgi.huffmanyarnmultithread.encoder;


import java.util.Arrays;


public class Codification {
	public byte symbol;
	public byte size;
	public byte lengthInBytes;
	public byte[] code;
	
	public Codification() {
	}

	public Codification(byte symbol, byte size, byte[] code) {
		this.symbol = symbol;
		this.size = size;
		this.lengthInBytes = (byte) (this.size + 2);
		this.code = new byte[this.size];
		this.code = Arrays.copyOf(code, this.size);
	}
	
	public Codification(byte[] byteArray, byte length) {
		this.symbol = byteArray[0];
		this.size = byteArray[1];
		this.code = Arrays.copyOfRange(byteArray, 2, length - 2);
	}

	public byte[] toByteArray() {
		byte[] b = new byte[this.size + 2];
		b[0] = this.symbol;
		b[1] = this.size;
		System.arraycopy(this.code, 0, b, 2, this.size);
		//for (int i = 0; i < this.size; i++)
		//	b[i + 2] = code[i];

		return b;
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(this.symbol + "(" + this.size + ") ");
		for(int i = 0 ; i < size ; i++) {
			if(this.code[i] == 0)
				stringBuilder.append("0");
			else
				stringBuilder.append("1");
		}
			
		return (stringBuilder.toString());
	}
}
