import sys
import re
import csv

def count_tokens(text):
    # Convert to lowercase, remove special characters, and split by whitespace
    return len(re.sub(r'[^\w\s]', ' ', text.lower()).split())

def process_document(doc_parts):
    if len(doc_parts) < 3:
        return False
    doc_id, title, content = doc_parts[0], doc_parts[1], doc_parts[2]
    length = count_tokens(content)
    print(f"{doc_id}\t{title}\t{length}")
    return True

for document in csv.reader(sys.stdin, delimiter='\t'):
    try:
        process_document(document)
    except Exception as e:
        if len(document) > 0:
            doc_id = document[0]
            sys.stderr.write(f"ERROR on doc_id {doc_id}: {str(e)}\n")
        else:
            sys.stderr.write(f"ERROR: {str(e)}\n")