import csv, re
import shutil
import sqlite3

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
    "\U0001FA00-\U0001FA6F"  # Chess Symbols
    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
    "\U00002702-\U000027B0"  # Dingbats
    "]+"
)

# NOTE: This script assumes that only internal python dependencies must be used to fulfill the requirements,
# with no need to install external 3rd-party modules
# For demo purposes, an emoji has been added to the input text in one row

# Source for stopwords: https://gist.github.com/sebleier/554280#file-nltk-s-list-of-english-stopwords
stopwords = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
             "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
             "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
             "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
             "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
             "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
             "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
             "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
             "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
             "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"}


def word_frequency_calculation(text_body):
    """

    :param text_body: the string body of the input sentence

    Returns Dict of word counts
    """
    words_dict = {}

    # split string on spaces and check every word
    for word in text_body.split():

        if (word not in stopwords) and (str(word).isnumeric() is False):

            if word not in words_dict.keys():  # add key with starting value if new

                words_dict[word] = 1

            else:

                words_dict[word] += 1  # otherwise increment

    # print(words_dict)

    #Convert word dict to list of tuples
    words_count = [(k, v) for k, v in words_dict.items()]
    return words_count


if __name__ == '__main__':

    """
    - Reads a CSV (comma-separated values) file from a folder (see description and sample on page 2)
    - Performs a word frequency count on the text in the “original_text” segment (the third segment)
    - The count should ignore a predefined set of common words (e.g. “a”, “the”, “then”, “and”,
    “an”, etc)
    - It should ignore numbers in numeric form within the text (e.g. “100”, “1.250”) but numbers
    spelled out as words (e.g. “three”, “thousand”) is allowed and should be counted
    - It should ignore emojis
    - The results should be stored in a database along with the original fields in the CSV file
    - The original file should be moved to another folder once processing is complete
    """

    input_file_ = 'in/ms_exercise_example.csv'

    # Define SQLite DB we will use to store the output
    database = 'exercise_database.db'
    input_table_name = 'input_text_reference_table'
    frequency_table = 'word_frequency_table'

    # Set up connection to our SQLite DB
    conn = sqlite3.connect(database)
    cur = conn.cursor()

    # Read in the CSV file
    with open(input_file_, 'r') as file:

        input_csv_reader = csv.reader(file, delimiter=',')
        next(input_csv_reader)  # skip over header, we will hardcode the names for this demo

        print("Creating tables....")

        # Database structure is as such:
        # One Table to contain the original text with the ID column as the primary key
        cur.execute(f"CREATE TABLE IF NOT EXISTS {input_table_name} ("
                    f"id INTEGER PRIMARY KEY,"
                    f"source TEXT NOT NULL,"
                    f"original_text TEXT NOT NULL);")

        # Another table to contain the word frequency count with the ID column of the original text body as the foreign key
        cur.execute(f"CREATE TABLE IF NOT EXISTS {frequency_table} ("
                    f"text_body_id INTEGER,"
                    f"word TEXT NOT NULL,"
                    f"count INTEGER,"
                    f"FOREIGN KEY (text_body_id) REFERENCES {input_table_name}(id))")

        # iterate through the rows, each one representing a text passage, we will performa a word count on each row
        for row in input_csv_reader:
            # parse the string and remove specified conditions
            id_ = row[0]
            input_string = str(row[2]).lower()
            input_string = re.sub(r'[^\w\s]', '', input_string)  # remove punctuation
            input_string = re.sub(EMOJI_PATTERN, '', input_string)  # remove emojis, this line is essentially a double tap to be sure

            # Write to the input table
            placeholders = ', '.join(['?' for _ in row])
            cur.execute(f"INSERT INTO {input_table_name} VALUES ({placeholders});", row)

            # Calculate the word frequency
            word_count_row = word_frequency_calculation(input_string)
            print(word_count_row)

            # Insert the dictionary for the text passage into the DB using executemany along with the id value
            sql = (f"INSERT into {frequency_table} (text_body_id, word, count)" + f" VALUES ({id_},?,?)")
            cur.executemany(sql, word_count_row)

        print("Database populated, commiting changes")

        # Create a TOTAL table that aggregates all instances of words in the frequency table
        # This segment was added in on the off chance a column-length word count was required
        cur.execute(f"""
        CREATE TABLE total_word_count AS SELECT word AS "word", SUM(count) AS "count" FROM {frequency_table}
        GROUP BY word;
        """)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

        # Finally, move the original file to the "out" directory
        shutil.move(input_file_, input_file_.replace("in/", 'out/'))

        print("Done")
