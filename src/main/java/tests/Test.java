package tests;

import joinPhase.JoinOneAndThreeGram;
import org.apache.hadoop.io.Text;
import sortPhase.Key;
import utils.DatasetFormat;
import utils.Utils;

import  static utils.Utils.*;

public class Test {
    public static void main(String[] args) {
        String example = "יצחק בר אבדימי\tn1\t1199";
        String example1 = "אחד הם\t8682";
//        System.out.println("<"+extractKey(new Text(example))+">");
//        System.out.println("<"+extractValue(new Text(example)).toString().split(DELIMITER)[0]+">");
//        System.out.println("<"+extractValue(new Text(example)).toString().split(DELIMITER)[1]+">");
//
//        System.out.println("<"+extractKey(new Text(example1))+">");
//        System.out.println("<"+extractValue(new Text(example1)).toString().split(DELIMITER)[0]+">");
//        System.out.println("< "+extractValue(new Text(example1)).toString().split(DELIMITER)[1]+">");

        Text value = new Text("אאבד את עצמי\t76");

        Text extractedKey = Utils.extractKey(value);
        String[] threeGram = extractedKey.toString().split(DatasetFormat.NGRAM_DELIMITER);
        Text keyW2 = new Text(threeGram[1]);
        Text keyW3 = new Text(threeGram[2]);
        Text valueW2 = new Text(JoinOneAndThreeGram.threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.C1IDENTIFIER);
        Text valueW3 = new Text(JoinOneAndThreeGram.threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.N1IDENTIFIER);
        Text valueC0 = new Text(JoinOneAndThreeGram.threeGramIdentifier + Utils.DELIMITER + extractedKey.toString() + Utils.DELIMITER + Utils.C0IDENTIFIER);

        System.out.println("<Key: <"+ keyW2 + ">  " + "Value: <"+valueW2 + ">");
        System.out.println("<Key: <"+ keyW3 + ">  " + "Value: <"+valueW3 + ">");
        System.out.println("<Key: <"+ Utils.C0KEY + ">  " + "Value: <"+valueC0 + ">");




    }
}
