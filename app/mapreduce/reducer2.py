import sys

def output_document_frequency(term, doc_ids):
    doc_count = len(doc_ids)
    print(f"{term}\t{doc_count}")

current_term = None
doc_ids = set()

for line in sys.stdin:
    try:
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue
            
        term, doc_id = parts
        
        if current_term and current_term != term:
            output_document_frequency(current_term, doc_ids)
            doc_ids = set()
        
        current_term = term
        doc_ids.add(doc_id)
        
    except Exception as e:
        sys.stderr.write(f"Error in reducer2: {str(e)}\n")

# Process last term
if current_term:
    output_document_frequency(current_term, doc_ids)