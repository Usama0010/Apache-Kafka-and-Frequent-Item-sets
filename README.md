# Apache-Kafka-and-Frequent-Item-sets
i191864_i221860_i221969_Usama_Danish_Mujahid

**Importing Libraries**
json: Used for handling JSON data.
os: Provides functions for interacting with the operating system.
tqdm: Displays a progress bar for iterations.
pandas: A powerful data manipulation library.
numpy: Library for numerical computations.
re: Provides support for regular expressions.
Loading Data
pd.read_json: Loads a JSON file into a Pandas DataFrame.
df1.head(): Displays the first 50 rows of the DataFrame.
Data Preprocessing
columns_to_keep: Defines the columns to keep in the DataFrame.
df[columns_to_keep]: Selects only the specified columns.
df['price'].astype(str): Converts the 'price' column to string type.
df['price'].str.extract(r'(\d+.\d+)').astype(float): Extracts floating-point numbers from the 'price' column.
df['price'].mean(): Calculates the mean of the 'price' column.
df['price'].fillna(mean_price, inplace=True): Fills missing values in the 'price' column with the mean.
transactions: Combines 'also_buy' and 'also_view' columns into a single 'transaction' column.
transactions['transaction']: Applies a lambda function to convert the 'transaction' column to sets.
df = df[df[col].astype(bool)]: Filters out rows where a specific column is empty.
df[col].apply(lambda x: [item.strip() for item in x]): Applies a lambda function to strip whitespace from items in a column.
Writing Filtered Data
df.to_json('filtered_data.json', orient='records', indent=4): Writes the filtered DataFrame to a JSON file.
Utility Functions
preprocess_batch(batch): Preprocesses a batch DataFrame by filtering out rows and applying transformations.
stream_data(): Streams data from a JSON file, preprocesses it in batches, and displays progress.
Main Function
if name == "main": Entry point of the script.
stream_data(): Calls the stream_data function to start processing the data

**Consumer 1**
extract_items(data): Extracts items from relevant fields in the JSON data.
generate_candidates(freq_itemsets, k): Generates candidate itemsets for the Apriori algorithm.
count_support(transactions, candidates): Counts the support for candidate itemsets.
apriori(transactions, min_support): Performs the Apriori algorithm on received transactions.
**Consumer 2**
extract_items(data): Extracts items from the JSON data.
count_item_pairs(transactions, hash_buckets, bucket_count): Counts item pairs and filters frequent pairs using the PCY algorithm.
filter_candidates(candidate_counts, support_threshold): Filters candidate item pairs based on support threshold.
pcy(transactions, support_threshold, hash_bucket_size): Performs the PCY algorithm on received transactions.
**Consumer 3**
IncrementalApriori: Class for performing incremental Apriori algorithm.
init(min_support): Initializes the minimum support threshold and itemsets dictionary.
extract_items(data): Extracts items from the JSON data.
update_itemsets(transaction): Updates the itemsets dictionary with new transactions.
prune_itemsets(): Prunes itemsets that do not meet the minimum support threshold.
print_frequent_itemsets(): Prints frequent itemsets and inserts results into MongoDB.
main: Entry point for the consumer.
Connects to Kafka and MongoDB.
Processes messages from Kafka, performs incremental Apriori, and inserts results into MongoDB.
Closes MongoDB connection.
**Producer**
IncrementalApriori: Similar to the consumer's IncrementalApriori class.
main: Entry point for the producer.
Connects to Kafka and MongoDB.
Processes messages from Kafka, performs incremental Apriori, and inserts results into MongoDB.
Closes MongoDB connection
