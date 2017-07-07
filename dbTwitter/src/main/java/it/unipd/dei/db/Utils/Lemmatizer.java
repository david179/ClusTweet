package it.unipd.dei.db.Utils;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.simple.Document;
import edu.stanford.nlp.simple.Sentence;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Collection of functions that allow to transform texts to sequence
 * of lemmas using lemmatization. An alternative process is
 * stemming. For a discussion of the difference between stemming and
 * lemmatization see this link: https://nlp.stanford.edu/IR-book/html/htmledition/stemming-and-lemmatization-1.html
 */
public class Lemmatizer {

  /**
   * Some symbols are interpreted as tokens. This regex allows us to exclude them.
   */
  public static Pattern symbols = Pattern.compile("^[',\\.`/-_]+$");
  
  /**
   * Web addresses are interpeted as token. We need to exclude them.
   */
  public static Pattern webAddr = Pattern.compile("^((http)|(https)).*$");
  
  /**
   * References to other user are interpeted as token. We need to exclude them.
   */
  public static Pattern ref = Pattern.compile("^@(.+)$");

  /**
   * A set of special tokens that are present in the Twitter dataset
   */
  public static HashSet<String> specialTokens =
    new HashSet<>(Arrays.asList("-lsb-", "-rsb-", "-lrb-", "-rrb-", "'s", "--"));

  /**
   * Transform a single document in the sequence of its lemmas.
   */
  public static ArrayList<String> lemmatize(String doc) {
    Document d = new Document(doc.toLowerCase());
    // Count spaces to allocate the vector to the right size and avoid trashing memory
    int numSpaces = 0;
    for (int i = 0; i < doc.length(); i++) {
      if (doc.charAt(i) == ' ') {
        numSpaces++;
      }
    }
    ArrayList<String> lemmas = new ArrayList<>(numSpaces);

    for (Sentence sentence : d.sentences()) {
      for (String lemma : sentence.lemmas()) {
        // Remove symbols
        if (!symbols.matcher(lemma).matches() && !specialTokens.contains(lemma) && !webAddr.matcher(lemma).matches()  && !ref.matcher(lemma).matches()) {
          lemmas.add(lemma);
        }
      }
    }

    return lemmas;
  }

  /**
   * Transform an RDD of strings in the corresponding RDD of lemma
   * sequences, with one sequence for each original document.
   */
  public static JavaRDD<ArrayList<String>> lemmatize(JavaRDD<String> docs) {
    return docs.map((d) -> lemmatize(d));
  }
  
  public static List<List<Word>> lemmatize2 (Iterable<String> itString){
      
    // break the tweet String into word and save each tweet as a List<Word>
  	// save all the tweets as a List of Lists
  	Scanner s;
  	List<List<Word>> sentences = new ArrayList();
	Iterator<String> it = itString.iterator();	

	while (it.hasNext())
	{
		s = new Scanner(it.next());
		s.useDelimiter(" ");
		List<Word> l = new ArrayList<Word>();
			
		while (s.hasNext())
		{
			String tmp = s.next().toLowerCase();
			if (!symbols.matcher(tmp).matches() && !specialTokens.contains(tmp) && !webAddr.matcher(tmp).matches()  && !ref.matcher(tmp).matches()) {
				l.add(new Word(tmp));
		    }
		}
		sentences.add(l);
	}
	
	return sentences;
  }


  public static void main(String[] args) {
    System.out.println(lemmatize("This is a sentence. This is another. The whole thing is a document made of sentences. http://bit.ly @ polli "));
  }
}
