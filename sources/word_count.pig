-- load the file from HDFS
lines = LOAD 'hdfs://localhost:19000/user/arvind/pig/acadgild/assignment_8.1/input' AS (line:chararray);

-- tokenize each line into words as separated by spaces. For every line this will generate a bag
-- To open out the lsit of words from the bag, use flatten
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- group the list of words attained earlier. All words which are same go into one group
words_grouped = GROUP words by word PARALLEL 5;

-- generate count of each group (that is each distinct word)
word_counts = FOREACH  words_grouped GENERATE group AS word , COUNT(words) AS word_count;

-- order the words and their counts in descending order of counts
-- Not asked for in the assignment. But will be useful to view the top n popular words
word_counts_ordered = ORDER word_counts BY word_count DESC PARALLEL 10;

-- finally store the output in HDFS
STORE word_counts_ordered INTO 'hdfs://localhost:19000/user/arvind/pig/acadgild/assignment_8.1/output';

