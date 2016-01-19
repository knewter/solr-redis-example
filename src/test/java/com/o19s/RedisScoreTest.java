package com.o19s;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisScoreTest extends LuceneTestCase {

	Field newFieldAllOn(String name, String value) {
		FieldType tagsFieldType = new FieldType();
		tagsFieldType.setStored(true);
		tagsFieldType.setIndexed(true);
		tagsFieldType.setOmitNorms(true);
		tagsFieldType.setStoreTermVectors(true);
		tagsFieldType.setStoreTermVectorPositions(true);
		tagsFieldType.setStoreTermVectorPayloads(true);
		return new Field(name, value, tagsFieldType);
	}
	
	TermQuery newTermQuery(String field, String search) {
		Term t = new Term(field, search);
		return new TermQuery(t);
	}
	
	IndexSearcher searcherUnderTest;
	RandomIndexWriter indexWriterUnderTest;
	IndexReader indexReaderUnderTest;
	Directory dirUnderTest;
	Jedis jedis;

	@Before
	public void setupIndex() throws IOException {
		dirUnderTest = newDirectory();
		jedis = new Jedis("localhost");
		
		indexWriterUnderTest = new RandomIndexWriter(random(), dirUnderTest);
		String[] docs = new String[] {
		  "star-trek star-wars space tv-shows",
		  "star-trek",
		  "tv-shows",
		  "star-trek tv-shows"
		};
		int[] docClicks = new int[] {
		  10,
		  2,
		  4,
		  1
		};
		for (int i = 0; i < docs.length; i++) {
			Document doc = new Document();
			String idStr = Integer.toString(i);
			doc.add(newFieldAllOn("id", idStr));
			doc.add(newFieldAllOn("tag", docs[i]));
			indexWriterUnderTest.addDocument(doc);
			// Here we're just storing data in redis in the key:
			// query|#{some_query_string}/id|#{document_id}
			// The value of this key represents the amount to boost based on clicks.
			// We can reduce click log data into this structure, and that algorithm
			// would be where we determined how much to count recency of clicks, etc.
			jedis.set("query|tag:star-trek/id|" + idStr, Integer.toString(docClicks[i]));
		}
		indexWriterUnderTest.commit();

		indexReaderUnderTest = indexWriterUnderTest.getReader();
		searcherUnderTest = newSearcher(indexReaderUnderTest);
	}

	@After
	public void closeStuff() throws IOException {
		indexReaderUnderTest.close();
		indexWriterUnderTest.close();
		dirUnderTest.close();

	}
	
	@Test
	public void testCountingScoring() throws IOException {
		TermQuery tq = newTermQuery("tag", "star-trek");
		RedisScoreQuery ct = new RedisScoreQuery(tq);
	
		TopDocs td = searcherUnderTest.search(ct, 10);
		ScoreDoc[] sdocs = td.scoreDocs;
				
		assert(sdocs[0].score == 10.0);
		assert(sdocs[1].score == 2.0);
		assert(sdocs[2].score == 1.0);
		
	}
}