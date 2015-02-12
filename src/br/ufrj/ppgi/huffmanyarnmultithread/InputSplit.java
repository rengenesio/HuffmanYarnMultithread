package br.ufrj.ppgi.huffmanyarnmultithread;


public class InputSplit {

	public int part;
	public long offset;
	public long length;

	public InputSplit() {
	}

	public InputSplit(int part, long offset, long length) {
		this.part = part;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public String toString() {
		return new String(this.part + "-" + this.offset + "-" + this.length); 
	}
}
