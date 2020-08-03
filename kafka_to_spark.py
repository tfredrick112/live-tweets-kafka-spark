from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.fpm import FPGrowth
import re

stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]
def split_tweet(line, fp):
    line = re.sub(r"[^\w\s]", '', line)
    split_line = line.split(" ")
    tweet = [word.lower() for word in split_line if len(word)>1]
    tweet = [word for word in tweet if word[:4]!='http']
    tweet = [word for word in tweet if word[0]!='@']
    tweet = [word for word in tweet if word not in stopwords]
    if fp:
        tweet = list(set(tweet))
    return tweet

def write_to_file(res_list):
    #f = open("/home/tony/Desktop/Semester6/ElecML/TwitterBigAssignment/outfile3.txt", 'a+')
    f = open("outputfile.txt", 'a+')
    for res in res_list[:10]:
        f.write(res[0]+" "+str(res[1])+"\n")
    f.close()

def write_number_of_tweets(n):
    # f = open("/home/tony/Desktop/Semester6/ElecML/TwitterBigAssignment/outfile3.txt", 'w')
    f = open("outputfile.txt", 'w')
    f.write("Number of tweets in the current batch = "+str(n)+"\n")
    f.close()

def custom_sort(x):
    # To sort based on the second element of a tuple (frequency)
    return x[1]

def frequent_word_comb(parsed):
    # Create the transaction-like dataset for FP-Growth
    transactions = parsed.map(lambda line: split_tweet(line, True))

    # Run the FPGrwoth algorithm with the specified minimum support and number of partitions
    model = FPGrowth.train(transactions, minSupport=0.01, numPartitions=4)
    result = model.freqItemsets().collect()
    # Select only frequent itemsets that have 2 or more elements.
    pair_results = [itemset for itemset in result if len(itemset[0])>=2]
    # Sort in the descending order of frequency
    pair_results.sort(reverse=True, key=custom_sort)
    f = open('outputfile.txt', "a+")
    for pair in pair_results[:10]:
        f.write("Frequent Itemset: "+str(pair[0])+"\tFrequency: "+str(pair[1])+"\n")
    f.close()

if __name__=='__main__':
    # Create the Spark context
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")

    # Set the log level
    sc.setLogLevel("OFF")
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)

    # Create the streaming context
    ssc = StreamingContext(sc, 60) # Set the time-interval to 60 seconds

    #kafkaStream = KafkaUtils.createStream(ssc, 'localhost:9092', 'cloudtony', topics={'CA':1})
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'cloudtony', topics={'CA':1})
    #kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['CA'], kafkaParams = {"bootstrap.servers": 'localhost:2181'})
    try:
        parsed = kafkaStream.map(lambda v: v[1])
        counts = parsed.flatMap(lambda line: split_tweet(line, False)).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
        sorted_counts = counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

        parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
        sorted_counts.pprint()

        parsed.count().foreachRDD(lambda rdd: write_number_of_tweets(rdd.collect()[0]))
        sorted_counts.foreachRDD(lambda rdd: write_to_file(rdd.collect()))
        parsed.foreachRDD(lambda rdd: frequent_word_comb(rdd))
    except:
        pass

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
