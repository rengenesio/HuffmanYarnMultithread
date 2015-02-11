package br.ufrj.ppgi.huffmanyarnmultithreaded.encoder;


public class BitSet {
	byte b;

	public BitSet() {
		b = 0;
	}

	public void setBit(int pos, boolean s) {
		pos = 7 - pos;
		if (s)
			b |= 1 << pos;
		else
			b &= ~(1 << pos);
	}

	public boolean cheackBit(int pos) {
		if (pos < 0) {
			System.out.println("erro!!!");
			System.exit(-1);
		}
		int bit = b & (1 << 7 - pos);
		if (bit > 0)
			return true;

		return false;
	}

	public void fromByte(byte b) {
		this.b = b;
	}

	@Override
	public String toString() {
		String s = new String();
		for (int i = 7 ; i >= 0 ; i--) {
			int bit = (b & (1 << i)) >> i;
			s += bit;
		}

		return s;
	}

}
