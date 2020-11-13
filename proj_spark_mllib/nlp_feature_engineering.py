from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, NGram, HashingTF, IDF, CountVectorizer

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .getOrCreate()


    df = spark.createDataFrame([
        (0, "From all sides were heard the footsteps and talk of the infantry, who were walking, driving past, and settling down all around."),
        (1, "IIt was no longer, as before, a dark, unseen river flowing through the gloom, but a dark sea swelling and gradually subsiding after a storm."),
        (2, "\"You don't mind your honor?\" he asked Tushin. \"I've lost my company, your honor. I don't know where... such bad luck!'\"")],
        "id int, message string")

    df.show(truncate=False)

    #A tokenizer that converts the input string to lowercase and then splits it by white spaces.
    words = Tokenizer(inputCol="message", outputCol="words").transform(df)
    words.show(truncate=False)


    # A regex based tokenizer that extracts tokens either by using the provided regex pattern (in Java dialect) to split the text (default) or repeatedly matching the regex (if gaps is false). Optional parameters also allow filtering tokens using a minimal length. It returns an array of strings that can be empty.
    words = RegexTokenizer(inputCol="message", outputCol="words", pattern="\\W+").transform(df)
    words.show(truncate=False)


    # StopWordsRemover is feature transformer that filters out stop words from input.
    stop_words_removed = StopWordsRemover(inputCol="words", outputCol="stop_words_removed").transform(words)
    stop_words_removed.show(truncate=False)


    # NGram is a feature transformer that converts the input array of strings into an array of n-grams. Null values in the input array are ignored. It returns an array of n-grams where each n-gram is represented by a space-separated string of words. When the input is empty, an empty array is returned. When the input array length is less than n (number of elements per n-gram), no n-grams are returned.
    ngram_df = NGram(n=2, inputCol="words", outputCol="ngrams").transform(words)

    ngram_df.show(truncate=False)
    ngram_df.select("ngrams").show(truncate=False)


    # TF-IDF is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus.[1] It is often used as a weighting factor in searches of information retrieval, text mining, and user modeling.
    df = words.select("words")
    df.show(truncate=False)

    # Hashing TF is TF with hashing enabled to allow the feature vector to be a set value
    df_tf = HashingTF(inputCol="words", outputCol="hashing_tf", numFeatures=15).transform(df)

    df_tf.show()
    df_tf.select("words").show(truncate=False)
    df_tf.select("hashing_tf").show(truncate=False)

    # IDF
    df_tf_idf = IDF(inputCol="hashing_tf", outputCol="tf_idf").fit(df_tf).transform(df_tf)

    df_tf_idf.show()
    df_tf_idf.select("words").show(truncate=False)
    df_tf_idf.select("hashing_tf").show(truncate=False) # Hashing TF
    df_tf_idf.select("tf_idf").show(truncate=False) # IDF


    # TF from CountVectorizer, which is used to extract words and counts from document collection
    df = words.select("words")
    df.show(truncate=False)


    df_tf_cv = CountVectorizer(inputCol="words", outputCol="tf_cv").fit(df).transform(df)
    df_tf_cv.show()
    df_tf_cv.select("words").show(truncate=False)
    df_tf_cv.select("tf_cv").show(truncate=False)
