package br.ufrj.ppgi.huffmanyarnmultithreaded.encoder;


import java.util.ArrayList;


public class NodeArray extends ArrayList<Node> {

	/**
         * 
         */
	private static final long serialVersionUID = 1L;

	public NodeArray(short size) {
		super(size);
	}

	public void insert(Node n) {
		short i;
		for (i = 0; i < super.size() && super.get(i).frequency > n.frequency; i++);
		super.add(i, n);
	}

	public void removeLastTwoNodes() {
		super.remove(super.size() - 1);
		super.remove(super.size() - 1);
	}

	public String toString() {
		String s = new String("NODE ARRAY: symbol(frequency)  left: [node]  right: [node]\n");
		for (Node n : this) {
			s += (n.toString() + "\n");
		}

		return s;
	}
}
