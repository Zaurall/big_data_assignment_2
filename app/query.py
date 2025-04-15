from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import sys
import os
import re
import math
import logging
from typing import List, Dict, Tuple, Set, Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("search-engine")

class CassandraConnector:
    """Handles connection to Cassandra database"""
    
    def __init__(self, host='cassandra-server', port=9042, keyspace='search_engine'):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        
    def connect(self):
        """Establish connection to Cassandra cluster"""
        try:
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect(self.keyspace)
            logger.info("Successfully connected to Cassandra cluster")
            return self.session
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            sys.exit(1)
            
    def close(self):
        """Close Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()


class QueryProcessor:
    """Processes search queries and retrieves results"""
    
    def __init__(self, session):
        self.session = session
        self.collection_stats = self._fetch_collection_statistics()
        
    def _fetch_collection_statistics(self) -> Dict[str, float]:
        """Fetch global statistics about the document collection"""
        doc_count = self._get_document_count()
        avg_length = self._get_avg_doc_length()
        
        logger.info(f"Collection statistics: {doc_count} documents, {avg_length:.2f} avg length")
        return {
            'doc_count': doc_count,
            'avg_length': avg_length
        }
        
    def _get_document_count(self) -> int:
        """Get the total number of documents in the collection"""
        try:
            rows = self.session.execute("SELECT COUNT(*) FROM documents_info")
            return rows.one()[0]
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            return 0
            
    def _get_avg_doc_length(self) -> float:
        """Calculate the average document length"""
        try:
            rows = self.session.execute("SELECT AVG(length) FROM documents_info")
            avg_length = rows.one()[0]
            return float(avg_length) if avg_length is not None else 0.0
        except Exception as e:
            logger.error(f"Failed to get average document length: {e}")
            return 0.0
            
    def normalize_query(self, query_text: str) -> List[str]:
        """Tokenize and normalize the query string"""
        query_text = query_text.lower()
        return re.findall(r'\w+', query_text)
        
    def fetch_term_statistics(self, term: str) -> Dict[str, Any]:
        """Get statistical information about a term"""
        doc_frequency = self._get_term_document_frequency(term)
        term_documents = self._get_documents_containing_term(term)
        
        return {
            'df': doc_frequency,
            'documents': term_documents
        }
        
    def _get_term_document_frequency(self, term: str) -> int:
        """Get document frequency (df) for a specific term"""
        try:
            query = SimpleStatement(
                "SELECT df FROM document_frequency WHERE term = %s",
                fetch_size=1
            )
            rows = self.session.execute(query, (term,))
            row = rows.one()
            return row.df if row else 0
        except Exception as e:
            logger.error(f"Failed to get document frequency for '{term}': {e}")
            return 0
            
    def _get_documents_containing_term(self, term: str) -> List[Tuple[str, int]]:
        """Get documents containing a specific term with term frequency"""
        try:
            query = SimpleStatement(
                "SELECT doc_id, tf FROM term_document WHERE term = %s",
                fetch_size=1000
            )
            rows = self.session.execute(query, (term,))
            return [(row.doc_id, row.tf) for row in rows]
        except Exception as e:
            logger.error(f"Failed to retrieve documents for term '{term}': {e}")
            return []
            
    def get_document_metadata(self, doc_ids: List[str]) -> Dict[str, Tuple[str, int]]:
        """Get document titles and lengths for a list of document IDs"""
        if not doc_ids:
            return {}
            
        try:
            placeholders = ', '.join(['%s'] * len(doc_ids))
            query = SimpleStatement(
                f"SELECT doc_id, title, length FROM documents_info WHERE doc_id IN ({placeholders})",
                fetch_size=len(doc_ids)
            )
            rows = self.session.execute(query, doc_ids)
            return {row.doc_id: (row.title, row.length) for row in rows}
        except Exception as e:
            logger.error(f"Failed to retrieve document metadata: {e}")
            return {}


class BM25Ranker:
    """Implements BM25 ranking algorithm for document retrieval"""
    
    def __init__(self, k1=1.0, b=0.75):
        self.k1 = k1
        self.b = b
        
    def compute_scores(self, spark_context, query_terms: List[str], processor: QueryProcessor) -> Tuple[Any, Dict]:
        """
        Compute BM25 scores for documents matching query terms
        """
        # Get collection statistics
        doc_count = processor.collection_stats['doc_count']
        avg_doc_length = processor.collection_stats['avg_length']
        
        # Process each query term
        term_doc_scores = []
        all_doc_ids = set()
        
        for term in query_terms:
            term_stats = processor.fetch_term_statistics(term)
            df = term_stats['df']
            
            # Skip terms that don't appear in any documents
            if df == 0:
                continue
                
            # Calculate IDF using probabilistic IDF
            idf = math.log(max(1, doc_count / max(1, df)))
            
            # Process each document containing this term
            for doc_id, tf in term_stats['documents']:
                term_doc_scores.append((doc_id, term, tf, idf))
                all_doc_ids.add(doc_id)
        
        # Retrieve metadata for all matching documents
        doc_metadata = processor.get_document_metadata(list(all_doc_ids))
        
        # Calculate per-term BM25 scores
        bm25_components = []
        for doc_id, term, tf, idf in term_doc_scores:
            if doc_id in doc_metadata:
                _, doc_length = doc_metadata[doc_id]
                
                # BM25 term-document score
                term_score = idf * ((tf * (self.k1 + 1)) / 
                           (tf + self.k1 * (1 - self.b + self.b * (doc_length / avg_doc_length))))
                
                bm25_components.append((doc_id, term, term_score))
        
        # Use Spark to aggregate scores by document
        scores_rdd = spark_context.parallelize(bm25_components)
        doc_scores = scores_rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda a, b: a + b)
        
        return doc_scores, doc_metadata


def display_search_results(results, doc_metadata, query):
    """Format and display search results to the user"""
    print("\nSearch Results for:", query)
    print("=" * 50)
    
    if not results:
        print("No relevant documents found.")
        return
        
    for i, (doc_id, score) in enumerate(results, 1):
        if doc_id in doc_metadata:
            title, _ = doc_metadata[doc_id]
            print(f"{i}. ID: {doc_id} Score: {score:.4f} Title: {title}")
        else:
            print(f"{i}. ID: {doc_id} Score: {score:.4f} Title: [Document details unavailable]")


def main():
    if len(sys.argv) <= 1:
        print("Please provide a search query as an argument")
        sys.exit(1)
    
    # Get the query from command line arguments
    query = ' '.join(sys.argv[1:])
    logger.info(f"Processing query: {query}")
    
    connector = CassandraConnector()
    session = connector.connect()
    
    spark = SparkSession.builder \
        .appName(f"BM25 Search: {query}") \
        .getOrCreate()
    
    try:
        processor = QueryProcessor(session)
        query_terms = processor.normalize_query(query)
        
        if not query_terms:
            print("No valid search terms found in the query")
            sys.exit(1)
            
        logger.info(f"Normalized query terms: {query_terms}")
        
        ranker = BM25Ranker()
        doc_scores, doc_metadata = ranker.compute_scores(
            spark.sparkContext, query_terms, processor
        )
        
        top_docs = doc_scores.takeOrdered(10, key=lambda x: -x[1])
        
        display_search_results(top_docs, doc_metadata, query)
        
    except Exception as e:
        logger.error(f"Error during search operation: {e}")
        
    finally:
        connector.close()
        spark.stop()

if __name__ == "__main__":
    main()