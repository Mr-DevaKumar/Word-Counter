import os
from flask import Flask, request, render_template
from collections import Counter
from multiprocessing import Pool

app = Flask(__name__)

# Mapper function
def mapper(data_chunk):
    """Processes a chunk of text and returns a word frequency Counter."""
    words = data_chunk.split()
    return Counter(words)

# Reducer function
def reducer(mapped_data):
    """Aggregates word frequencies from multiple mappers."""
    aggregated = Counter()
    for data in mapped_data:
        aggregated.update(data)
    return aggregated

# Function to split dataset into chunks
def split_dataset(data, num_chunks):
    """Splits the input dataset into approximately equal chunks."""
    chunk_size = len(data) // num_chunks
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

# Distributed word count function
def distributed_word_count(data, num_mappers):
    """Coordinates the distributed word counting process."""
    # Split dataset into chunks
    chunks = split_dataset(data, num_mappers)

    # Create a pool of worker processes
    with Pool(num_mappers) as pool:
        # Map phase: Process chunks in parallel
        mapped_results = pool.map(mapper, chunks)

        # Reduce phase: Aggregate results from all mappers
        final_result = reducer(mapped_results)

    return final_result

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/count', methods=['POST'])
def count():
    # Retrieve the uploaded file and number of mappers
    uploaded_file = request.files['file']
    num_mappers = int(request.form['num_mappers'])

    if uploaded_file:
        # Read file content
        file_content = uploaded_file.read().decode('utf-8')

        # Perform distributed word count
        word_frequencies = distributed_word_count(file_content, num_mappers)

        # Convert results to a sorted list of tuples
        sorted_frequencies = word_frequencies.most_common(20)  # Top 20 words

        return render_template('results.html', word_frequencies=sorted_frequencies)

    return "No file uploaded. Please try again."

if __name__ == "__main__":
    app.run(debug=True)
