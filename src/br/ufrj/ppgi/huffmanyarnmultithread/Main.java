package br.ufrj.ppgi.huffmanyarnmultithread;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithread.decoder.Decoder;
import br.ufrj.ppgi.huffmanyarnmultithread.yarn.Client;


public class Main {

	public static void main(String[] args) throws Exception {
		long t, t1, t2;
		String in, out, cb;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		in = new String(args[0]);
		out = new String(in);
		cb = new String(in);
		out += ".yarnmultithreaddir/compressed/";
		cb += ".yarnmultithreaddir/codification";

		try {
			fs.delete(new Path(args[0] + ".yarnmultithreaddir"), true);
		} catch(Exception ex) { }
			

		t1 = System.nanoTime();
		Client client = new Client(args);
		
		if (client.run()) {
			System.out.println("Compressão completa!");
		}
		else {
			System.out.println("Erro durante a compressão");
		}
		
		t2 = System.nanoTime();
		t = t2 - t1;
		System.out.println(t/1000000000.0 + " s (encoder)");

//		in = new String(args[0]);
//		out = new String(in);
//		cb = new String(in);
//		in += ".yarnmultithreaddir/compressed/";
//		out += ".yarnmultithreaddir/decompressed";
//		cb += ".yarnmultithreaddir/codification";
//		
//		System.out.println(in);
//		System.out.println(out);
//		System.out.println(cb);
//		
//		t1 = System.nanoTime();
//		new Decoder(in, out, cb);
//		t2 = System.nanoTime();
//		t = t2 - t1;
//		System.out.println(t/1000000000.0 + " s (decoder)");
	}
}
