package br.ufrj.ppgi.huffmanyarnmultithreaded.encoder;


public class Node {
    public Node left, right;
    byte symbol;
    long frequency;
    boolean visited;

    public Node(byte symbol, long frequency, Node left, Node right) {
            this.symbol = symbol;
            this.frequency = frequency;
            this.left = left;
            this.right = right;
    }

    public Node(byte symbol, long frequency) {
            this(symbol, frequency, null, null);
    }

    public Node() {
            this((byte) 0, (long) 0, null, null);
    }

    public String toString() {
            String s = new String();
            s += (this.symbol + "(" + this.frequency + ") left: [");
            if (this.left != null)
                    s += (left.symbol + "(" + left.frequency + ")] right: [");
            else
                    s += "N] right: [";

            if (this.right != null)
                    s += (right.symbol + "(" + right.frequency + ")]");
            else
                    s += "N";

            return s;
    }
}