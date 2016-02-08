package com.o19s;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import redis.clients.jedis.Jedis;

public class RedisScoreQuery extends CustomScoreQuery {
  private Query query;
  private Jedis jedis;

  public RedisScoreQuery(Query subQuery) {
    super(subQuery);
    this.query = subQuery;
    this.jedis = new Jedis("localhost");
  }


  public class RedisScoreQueryScoreProvider extends CustomScoreProvider {
    String _field;

    public RedisScoreQueryScoreProvider(String field, AtomicReaderContext context) {
      super(context);
      _field = field;
    }

    // Rescores by finding a key in redis to look for a value in.  This way we can boost based on whatever we'd like.  The key is query-and-document-specific.
    public float customScore(int doc, float subQueryScore, float valSrcScores[]) throws IOException {
      Set<Term> terms = new HashSet<>();
      query.extractTerms(terms);

      String queryDocKey = this.getQueryDocKey(terms, doc);

      String value = "0";
      if(jedis.get(queryDocKey) != null){
        value = jedis.get(queryDocKey);
      };

      return Float.valueOf(value);
    }

    private String getQueryDocKey(Set<Term> terms, int doc) throws IOException {
      Iterator<Term> iter = terms.iterator();
      String key = "query|";

      while(iter.hasNext()){
        key = key + iter.next().toString();
      }

      key = key + "/id|";

      IndexReader r = context.reader();
      Terms tv = r.getTermVector(doc, "id");

      TermsEnum termsEnum = null;
      termsEnum = tv.iterator(termsEnum);

      termsEnum.next();
      BytesRef currentTerm = termsEnum.term();
      key = key + currentTerm.utf8ToString();

      return key;
    }
  }

  protected CustomScoreProvider getCustomScoreProvider(AtomicReaderContext context) throws IOException {
    return new RedisScoreQueryScoreProvider("tag", context);
  }
}
