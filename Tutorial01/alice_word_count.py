from pyspark import SparkContext, SparkConf

import re

abbreviated_terms = {
    "aren't": ["are", "not"],
    "can't": ["cannot"],
    "couldn't": ["could", "not"],
    "didn't": ["did", "not"],
    "doesn't": ["does", "not"],
    "don't": ["do", "not"],
    "hadn't": ["had", "not"],
    "hasn't": ["has", "not"],
    "haven't": ["have", "not"],
    "i'm": ["i","am"],
    "i've": ["i", "have"],
    "isn't":['is', "not"],
    "let's": ["let", "us"],
    "mustn't": ["must", "not"],
    "shan't": ["shall", "not"],
    "shouldn't":["should", "not"],
    "they're":["they", "are"],
    "they've":["they", "have"],
    "they'll":["they", "will"], 
    "wasn't":["was","not"],
    "we'll": ["we", "will"],
    "we're": ["we", "are"],
    "we've": ["we", "have"],
    "weren't": ["were", "not"],
    "what're": ["what", "are"],
    "who're": ["who", "are"],
    "who've": ["who", "have"],
    "won't": ["will", "not"],
    "wouldn't": ["would", "not"],
    "you're": ["you", "are"],
    "you've": ["you","have"]
    }

def split_words(text):
    words = text.split(" ")
    clean_words = list()

    for word in words:
        #make all words lower case
        word = word.lower()
        
        #remove puncatuation
        word = remove_punctuation(word)

        #remove abbreviated terms
        if "'" in word and word in abbreviated_terms:
            clean_words.extend(abbreviated_terms[word])
        else:
            clean_words.append(word)
    return clean_words

def remove_punctuation(word):
    word = re.sub('[,|:|`|;|(|)|!|?|.|\"|[|*|_]',"",word)
    word = re.sub("--", "", word)

    if word.endswith("'"):
        word = word[:-1]
    if word.startswith("'"):
        word = word[1:]
    return word
        

def main():
    conf = SparkConf().setAppName("AliceWordCount")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    rdd = sc.textFile("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial01/alice.txt")

    #split words
    words = rdd.flatMap(split_words)

    #remove empty words:
    words = words = words.filter(lambda word: len(word) > 0)

    #map words to key-value pairs and count:
    words = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    words = words.coalesce(1)

    #sort alphabetically:
    words = words.sortByKey(True)


    words.saveAsTextFile("/Users/faizan/Documents/Masters/2nd_Semester/Big_Data/Tutorial/Tutorials/Tutorial01/aliceWordCount")

    sc.stop()


if __name__ == "__main__":
    main()