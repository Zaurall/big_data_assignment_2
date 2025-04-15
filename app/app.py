from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
import sys
import logging
import re
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def count_tokens(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    tokens = text.split()
    return len(tokens)

def connect_to_cassandra(host='cassandra-server', port=9042):
    """Connect to Cassandra cluster"""
    try:
        cluster = Cluster([host], port=port)
        session = cluster.connect()
        logger.info("Successfully connected to Cassandra cluster")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        sys.exit(1)

def create_keyspace_and_tables(session, keyspace_name='search_engine'):
    """Create keyspace and tables for storing index data"""
    try:
        # Create keyspace
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
            WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
        """)
        
        # Use keyspace
        session.execute(f"USE {keyspace_name}")
        
        # Create term index table
        session.execute("""
            CREATE TABLE IF NOT EXISTS term_document (
                term text,
                doc_id text,
                tf int,
                positions list<int>,
                PRIMARY KEY (term, doc_id)
            )
        """)
        
        # Create document metadata table
        session.execute("""
            CREATE TABLE IF NOT EXISTS documents_info (
                doc_id text PRIMARY KEY,
                title text,
                length int
            )
        """)
        
        # Create document frequency table for BM25
        session.execute("""
            CREATE TABLE IF NOT EXISTS document_frequency (
                term text PRIMARY KEY,
                df int
            )
        """)
        
        logger.info("Successfully created keyspace and tables")
    except Exception as e:
        logger.error(f"Failed to create keyspace and tables: {e}")
        sys.exit(1)

def read_hdfs_file(hdfs_path):
    """Read a file from HDFS"""
    try:
        cmd = ["hdfs", "dfs", "-cat", hdfs_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to read file {hdfs_path} from HDFS: {e}")
        return ""

def list_hdfs_files(hdfs_path):
    """List files in an HDFS directory"""
    try:
        cmd = ["hdfs", "dfs", "-ls", hdfs_path]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse output to get file paths
        lines = result.stdout.strip().split('\n')
        files = []
        
        for line in lines:
            if not line.startswith('Found') and line.strip():
                # Extract the file path (last component after whitespace)
                parts = line.strip().split()
                if len(parts) >= 8:
                    files.append(parts[-1])
        
        return files
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to list files in {hdfs_path}: {e}")
        return []
    
def execute_batch_safely(session, batch, statements=None):
    """Helper function to execute a batch safely, falling back to individual inserts if needed."""
    try:
        # Check if batch has any statements before executing            
        if len(batch) > 0:
            session.execute(batch)
    except Exception as e:
        if "Batch too large" in str(e) and statements:
            logger.warning("Batch too large, inserting entries one by one")
            for stmt, params in statements:
                session.execute(stmt, params)
        else:
            raise e
    return BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

def import_term_document(session):
    """Import term index data from HDFS into Cassandra without Spark"""
    try:
        hdfs_path = "/tmp/index/step1"
        logger.info(f"Reading term index data from {hdfs_path}")
        
        part_files = [f for f in list_hdfs_files(hdfs_path) if "part-" in f]
        
        batch_size = 5
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statements = []
        count = 0
        
        for file_path in part_files:
            content = read_hdfs_file(file_path)
            
            for line in content.split('\n'):
                if not line.strip():
                    continue
                    
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    term, doc_id, tf, positions_str = parts[0], parts[1], int(parts[2]), parts[3]
                    positions = [int(pos) for pos in positions_str.split(',') if pos]
                    
                    stmt = "INSERT INTO term_document (term, doc_id, tf, positions) VALUES (%s, %s, %s, %s)"
                    params = (term, doc_id, tf, positions)
                    batch.add(stmt, params)
                    statements.append((stmt, params))
                    
                    count += 1
                    if count % batch_size == 0:
                        batch = execute_batch_safely(session, batch, statements)
                        statements = []
        
        # Execute any remaining batch items
        if len(batch) > 0:
            execute_batch_safely(session, batch, statements)    
        
        logger.info(f"Successfully imported {count} term index entries")
    except Exception as e:
        logger.error(f"Failed to import term index data: {e}")
        sys.exit(1)

def import_document_frequency(session):
    """Import document frequency data from HDFS into Cassandra without Spark"""
    try:
        hdfs_path = "/tmp/index/step2"
        logger.info(f"Reading document frequency data from {hdfs_path}")
        
        part_files = [f for f in list_hdfs_files(hdfs_path) if "part-" in f]
        
        batch_size = 50
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statements = []
        count = 0
        
        for file_path in part_files:
            content = read_hdfs_file(file_path)
            
            for line in content.split('\n'):
                if not line.strip():
                    continue
                    
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    term, df = parts[0], int(parts[1])

                    stmt = "INSERT INTO document_frequency (term, df) VALUES (%s, %s)"
                    params = (term, df)
                    batch.add(stmt, params)
                    statements.append((stmt, params))
                    
                    count += 1
                    if count % batch_size == 0:
                        batch = execute_batch_safely(session, batch, statements)
                        statements = []
        
        # Execute any remaining batch items
        if len(batch) > 0:
            execute_batch_safely(session, batch, statements)    
        
        logger.info(f"Successfully imported {count} document frequency entries")
    except Exception as e:
        logger.error(f"Failed to import document frequency data: {e}")
        sys.exit(1)

def import_document_metadata(session):
    """Import document metadata from MapReduce output into Cassandra"""
    try:
        logger.info("Importing document metadata from MapReduce output")
        
        hdfs_path = "/tmp/index/step3"
        part_files = [f for f in list_hdfs_files(hdfs_path) if "part-" in f]
        
        batch_size = 25
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        statements = []
        count = 0
        
        for file_path in part_files:
            content = read_hdfs_file(file_path)
            
            for line in content.split('\n'):
                if not line.strip():
                    continue
                    
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    doc_id, title, length = parts[0], parts[1], int(parts[2])
                    
                    stmt = "INSERT INTO documents_info (doc_id, title, length) VALUES (%s, %s, %s)"
                    params = (doc_id, title, length)
                    batch.add(stmt, params)
                    statements.append((stmt, params))
                    
                    count += 1
                    if count % batch_size == 0:
                        batch = execute_batch_safely(session, batch, statements)
                        statements = []
        
        
        # Execute any remaining batch items
        if len(batch) > 0:
            execute_batch_safely(session, batch, statements)
        
        logger.info(f"Successfully imported {count} document metadata entries")
    except Exception as e:
        logger.error(f"Failed to import document metadata: {e}")
        sys.exit(1)

def main():
    cluster, session = connect_to_cassandra()

    create_keyspace_and_tables(session)
    
    import_term_document(session)
    import_document_frequency(session)
    import_document_metadata(session)
    
    cluster.shutdown()
    
    logger.info("Successfully imported index data into Cassandra")

if __name__ == "__main__":
    main()