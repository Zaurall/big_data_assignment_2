import sys

def extract_term_doc(line):
    parts = line.strip().split('\t')
    if len(parts) >= 2:
        term, doc_id = parts[0], parts[1]
        return term, doc_id
    return None

for line in sys.stdin:
    try:
        result = extract_term_doc(line)
        if result:
            term, doc_id = result
            print(f"{term}\t{doc_id}")
    except Exception as e:
        sys.stderr.write(f"Error in mapper2: {str(e)}\n")