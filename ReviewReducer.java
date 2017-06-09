package yelp_season;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.tools.ant.types.CommandlineJava.SysProperties;

public class ReviewReducer extends Reducer<Text, Text, Text, Text> {
	double[] counts = new double[11];
	HashMap<String, Node> map = new HashMap<String, Node>();
	ArrayList<String> keys = new ArrayList<String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			File f = new File(key.toString());
			FileWriter fw = new FileWriter(f);

			System.out.println("starting red");

			for (Text x : values) {

				String[] array = x.toString().split("~~");
				System.out.println("\n\n\n\n");
				System.out.println("d");
				System.out.println(key.toString() + "\t\t\t" + x.toString());
				for (String test : array) {
					System.out.println(test);
				}
				System.out.println("/d");
				System.out.println("\n\n\n\n");
				if(array[1].equals( "11")){
					System.out.println(x.toString());
				}
				int[] date = date(array[1], array);

				if (map.containsKey(date[0] + "~~" + date[1])) {
					map.get(date[0] + "~~" + date[1]).ratings[Integer.parseInt(array[0])]++;
				} else {
					Node temp = new Node();
					temp.ratings[Integer.parseInt(array[0])]++;
					map.put(date[0] + "~~" + date[1], temp);
					keys.add(date[0] + "~~" + date[1]);
				}
			}

			for (String x : keys) {
				Node temp = map.get(x);

				int count = 0;
				BigDecimal dividend = new BigDecimal("0");
				BigDecimal divisor = new BigDecimal("0");
				for (long y : temp.ratings) {
					divisor = divisor.add(new BigDecimal("" + temp.ratings[count]));
					dividend = dividend.add(new BigDecimal("" + (count * temp.ratings[count])));
					count++;
				}

				Text valOut = new Text("totalRevs~~" + divisor + "~~" + "avg" + "~~"
						+ dividend.divide(divisor, MathContext.DECIMAL128).toString());
				Text keyOut = new Text(key.toString() + "~~" + x);
				System.out.println("Reduce! " + keyOut + " " + valOut);
				context.write(keyOut, valOut);
				fw.append(keyOut.toString() + "\t" + valOut.toString() + "\n");
			}
			fw.close();
		} catch (FileNotFoundException e) {
			System.out.println(key.toString());
			System.out.println("sfdfbawffd");
			System.exit(0);
		}

		context.write(new Text(), new Text());

	}

	private int[] date(String s, String[] ar) {
		try {
			
			String[] temp = s.split("-");
			int[] date = new int[2];
			if(temp.length == 0){
				System.out.println(s);
				System.out.println("  2  ");	
			}
			if(temp.length == 1){
				System.out.println(s);
				System.out.println(" 1  ");
			}
			date[0] = Integer.parseInt(temp[0]);
			date[1] = Integer.parseInt(temp[1]);
			return date;
		} catch (NumberFormatException e) {
			String[] temp = s.split("-");
			for (String sd : ar) {
				System.out.println("error!!!!    " + sd);
			}
			System.exit(0);
		}
		return new int[] {};
	}

	private class Node {
		long[] ratings;

		public Node() {
			ratings = new long[] { 0, 0, 0, 0, 0, 0 };
		}
	}

}