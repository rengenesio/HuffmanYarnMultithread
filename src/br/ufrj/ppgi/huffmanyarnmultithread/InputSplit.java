package br.ufrj.ppgi.huffmanyarnmultithread;


public class InputSplit implements Comparable<InputSplit> {

	public int part;
	public long offset;
	public int length;

	public InputSplit() {
	}

	public InputSplit(int part, long offset, int length) {
		this.part = part;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public String toString() {
		return new String("InputSplit: " + this.part + "-" + this.offset + "-" + this.length); 
	}

	@Override
	public int compareTo(InputSplit anotherInputSplit) {
		if(this.part < anotherInputSplit.part) {
			return -1;
		} else if (this.part == anotherInputSplit.part) {
			return 0;	
		} else if (this.part > anotherInputSplit.part) {
			return 1;	
		}
		
		return 0;
	}
}
