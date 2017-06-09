package yelp_season;

import com.mongodb.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
	String targetCat = "Italian";
	String[] cate = new String[] { "Afghan", "African", "Senegalese", "American (New)", "American (Traditional)",
			"Arabian", "Argentine", "Armenian", "Asian Fusion", "Australian", "Austrian", "Bangladeshi", "Barbeque",
			"Belgian", "Brazilian", "Breakfast & Brunch", "British", "Buffets", "Burgers", "Burmese", "Cafes",
			 "Cambodian", "Caribbean", "Dominican", "Haitian", "Puerto Rican",
			"Trinidadian", "Chicken Shop", "Chicken Wings", "Chinese", "Comfort Food", "Creperies", "Cuban", "Czech",
			"Delis", "Diners", "Dinner Theater", "Ethiopian", "Fast Food", "Filipino", "Fish & Chips", "Food Court",
			"Food Stands", "French", "Game Meat", "German", "Gluten-Free", "Greek", "Halal", "Hawaiian",
			 "Honduran", "Hot Dogs", "Hot Pot", "Hungarian", "Indian", "Indonesian", "Irish",
			"Italian", "Tuscan", "Japanese", "Conveyor Belt Sushi", "Japanese Curry", "Ramen", "Kebab", "Korean",
			"Kosher", "Laotian", "Latin American", "Colombian", "Salvadoran", "Venezuelan",
			"Malaysian", "Mediterranean", "Falafel", "Mexican", "Tacos", "Middle Eastern", "Egyptian", "Lebanese",
			"Modern European", "Mongolian", "Moroccan", "Nicaraguan", "Noodles", "Pakistani", "Pan Asian",
			"Persian/Iranian", "Peruvian", "Pizza", "Polish", "Pop-Up Restaurants", "Portuguese", "Poutineries",
			"Russian", "Salad", "Sandwiches", "Scandinavian", "Scottish", "Seafood", "Singaporean", "Soul Food", "Soup",
			"Southern", "Spanish", "Sri Lankan", "Steakhouses", "Sushi Bars", "Syrian", "Taiwanese", "Tex-Mex", "Thai",
			"Turkish", "Ukrainian", "Vegan", "Vegetarian", "Vietnamese", "Waffles", "Wraps" };
	HashSet dict = new HashSet(Arrays.asList(cate));

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] idStarDateCats = value.toString().split("~~");

		if ((idStarDateCats.length > 3)) {
			
			int count = 0;
			String[] cats = new String[idStarDateCats.length - 3];
			for (int i = 3; i < idStarDateCats.length; i++) {
				cats[count] = idStarDateCats[i];
				count++;
			}

			
			
			String output = "" + (Long) Long.parseLong(idStarDateCats[1]) + "~~" + idStarDateCats[2];
			Text valOut = new Text(output);

			for (String x : cats) {
				if (dict.contains(x) && (x.startsWith(targetCat) )) {

					System.out.println(x.toString());

					Text keyOut = new Text(x);

					System.out.println("Map! " + keyOut + "                   " + valOut);
					context.write(keyOut, valOut);
				}
			}

		}
	}

}
