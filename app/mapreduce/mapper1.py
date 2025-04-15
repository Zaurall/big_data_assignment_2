import sys
import re
import csv

def tokenize_text(text):
    # Convert to lowercase, remove special characters, and split by whitespace
    tokens = re.sub(r'[^\w\s]', ' ', text.lower()).split()
    # You can additionally remove tokens that are short or non-alphabetic 
    # tokens = [token for token in tokens if len(token) > 2 and token.isalpha()]
    return tokens

def process_document(doc_parts):
    if len(doc_parts) < 3:
        return
    doc_id, _, content = doc_parts[0], doc_parts[1], doc_parts[2]
    tokens = tokenize_text(content)
    for position, term in enumerate(tokens):
        print(f"{term}\t{doc_id}\t{position}")

for document in csv.reader(sys.stdin, delimiter='\t'):
    try:
        process_document(document)
    except Exception as e:
        sys.stderr.write(f"Error processing line: {str(e)}\n")
        continue